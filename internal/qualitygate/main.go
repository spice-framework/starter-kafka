// Command qualitygate runs starter-kafka's repository-owned cross-platform checks.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	requiredGoVersion = "go1.26.5"
	modulePath        = "github.com/spice-framework/starter-kafka"
	spiceModulePath   = "github.com/spice-framework/spice"
	minimumCoverage   = 85.0
	compatibilityFile = "spice-compatibility.json"
	compatibilityV1   = 1
)

var output = log.New(os.Stdout, "", 0)

func main() {
	os.Exit(execute()) // Entrypoint exception: propagate verification failure.
}

func execute() int {
	mode := flag.String(
		"mode",
		"verify",
		"verification mode: check, compatibility, fmt, lint, release-parity, security, verify, or verify-release",
	)
	compatibilityLine := flag.String("line", "all", "Spice compatibility line: minimum, current, or all")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	root, err := repositoryRoot()
	if err == nil {
		err = run(ctx, root, *mode, *compatibilityLine)
	}
	if err != nil {
		output.Printf("quality gate failed: %v", err)
		return 1
	}
	return 0
}

type step struct {
	name string
	run  func() error
}

func run(ctx context.Context, root, mode, compatibilityLine string) error {
	if runtime.Version() != requiredGoVersion {
		return fmt.Errorf("go version is %s; require exactly %s", runtime.Version(), requiredGoVersion)
	}
	identity := step{"repository identity", func() error { return checkIdentity(ctx, root) }}
	dependencies := step{"dependency and module preparation", func() error {
		return prepareDependencies(ctx, root)
	}}
	preparationLine := compatibilityLine
	if mode == "verify" {
		preparationLine = "all"
	}
	compatibilityDependencies := step{"Spice compatibility preparation", func() error {
		return prepareCompatibilityDependencies(ctx, root, preparationLine)
	}}
	formatting := step{"formatting", func() error { return format(ctx, root, false) }}
	modules := step{"module and vendor", func() error { return checkModule(ctx, root) }}
	vet := step{"go vet", func() error { return command(ctx, root, nil, "go", "vet", "./...") }}
	release := step{"central and retained release parity", func() error {
		return releaseParity(ctx, root)
	}}
	var steps []step
	switch mode {
	case "check":
		steps = []step{identity, dependencies, formatting, modules, vet}
	case "compatibility":
		steps = []step{
			identity,
			compatibilityDependencies,
			{"Spice core compatibility", func() error {
				return coreCompatibility(ctx, root, compatibilityLine)
			}},
		}
	case "fmt":
		steps = []step{{"formatting", func() error { return format(ctx, root, true) }}}
	case "lint":
		steps = []step{
			identity,
			dependencies,
			formatting,
			{"lint and nil safety", func() error { return lint(ctx, root) }},
		}
	case "release-parity":
		steps = []step{identity, release}
	case "security":
		steps = []step{
			identity,
			dependencies,
			{"security", func() error { return security(ctx, root) }},
		}
	case "verify":
		steps = []step{
			identity,
			dependencies,
			compatibilityDependencies,
			formatting,
			modules,
			vet,
			{"lint and nil safety", func() error { return lint(ctx, root) }},
			{"security", func() error { return security(ctx, root) }},
			{"shuffled and race tests", func() error { return tests(ctx, root) }},
			{"coverage", func() error { return coverage(ctx, root) }},
			{"Spice core compatibility", func() error { return coreCompatibility(ctx, root, "all") }},
			{"offline vendor", func() error { return offline(ctx, root) }},
		}
	case "verify-release":
		steps = []step{identity, release}
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
	for _, current := range steps {
		started := time.Now()
		output.Printf("==> %s", current.name)
		if err := current.run(); err != nil {
			return fmt.Errorf("%s (%s): %w", current.name, time.Since(started).Round(time.Millisecond), err)
		}
		output.Printf("<== %s passed in %s", current.name, time.Since(started).Round(time.Millisecond))
	}
	output.Print("==> all verification passed")
	return requireReleaseTool(ctx, root)
}

func prepareDependencies(ctx context.Context, root string) error {
	if err := networkCommand(ctx, root, "mod", "download"); err != nil {
		return err
	}
	if err := networkCommand(ctx, root, "-C", "tools", "mod", "download"); err != nil {
		return err
	}
	// A tools module's tidy graph includes test-only dependencies of tool
	// packages. They are intentionally not fetched by `go mod download`, so the
	// read-only tidy check belongs in this explicit network-capable phase.
	if err := networkCommand(ctx, root, "-C", "tools", "mod", "tidy", "-diff"); err != nil {
		return err
	}
	return nil
}

func checkIdentity(ctx context.Context, root string) error {
	content, err := os.ReadFile(filepath.Join(root, "go.mod")) // #nosec G304 -- root is resolved from this repository's module identity.
	if err != nil {
		return fmt.Errorf("read go.mod: %w", err)
	}
	if !strings.Contains(string(content), "module "+modulePath+"\n") {
		return fmt.Errorf("go.mod does not declare canonical module %s", modulePath)
	}
	if bytes.Contains(content, []byte("\nreplace ")) || bytes.Contains(content, []byte("\nreplace (")) {
		return errors.New("committed go.mod must not contain replace directives")
	}
	versions, err := readCompatibility(root)
	if err != nil {
		return err
	}
	minimum, err := directRequirement(ctx, root, spiceModulePath)
	if err != nil {
		return err
	}
	if minimum != versions.Minimum {
		return fmt.Errorf(
			"go.mod directly requires %s at %s; compatibility minimum is %s",
			spiceModulePath, minimum, versions.Minimum,
		)
	}
	return nil
}

type compatibilityVersions struct {
	Schema  int    `json:"schema"`
	Minimum string `json:"minimum"`
	Current string `json:"current"`
}

func readCompatibility(root string) (compatibilityVersions, error) {
	content, err := os.ReadFile(filepath.Join(root, compatibilityFile)) // #nosec G304 -- the repository root and filename are fixed.
	if err != nil {
		return compatibilityVersions{}, fmt.Errorf("read %s: %w", compatibilityFile, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var result compatibilityVersions
	if err := decoder.Decode(&result); err != nil {
		return compatibilityVersions{}, fmt.Errorf("decode %s: %w", compatibilityFile, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return compatibilityVersions{}, fmt.Errorf("%s has trailing JSON values", compatibilityFile)
		}
		return compatibilityVersions{}, fmt.Errorf("decode trailing %s content: %w", compatibilityFile, err)
	}
	if result.Schema != compatibilityV1 {
		return compatibilityVersions{}, fmt.Errorf("%s schema %d is unsupported", compatibilityFile, result.Schema)
	}
	if strings.TrimSpace(result.Minimum) == "" || strings.TrimSpace(result.Current) == "" {
		return compatibilityVersions{}, fmt.Errorf("%s requires explicit minimum and current versions", compatibilityFile)
	}
	if strings.TrimSpace(result.Minimum) != result.Minimum || strings.TrimSpace(result.Current) != result.Current {
		return compatibilityVersions{}, fmt.Errorf("%s versions must not contain surrounding whitespace", compatibilityFile)
	}
	if result.Minimum == result.Current {
		return compatibilityVersions{}, fmt.Errorf("%s minimum and current versions must differ", compatibilityFile)
	}
	return result, nil
}

type compatibilityBoundary struct {
	Name    string
	Version string
}

func (versions compatibilityVersions) boundaries(line string) ([]compatibilityBoundary, error) {
	switch line {
	case "minimum":
		return []compatibilityBoundary{{Name: "minimum", Version: versions.Minimum}}, nil
	case "current":
		return []compatibilityBoundary{{Name: "current", Version: versions.Current}}, nil
	case "all":
		return []compatibilityBoundary{
			{Name: "minimum", Version: versions.Minimum},
			{Name: "current", Version: versions.Current},
		}, nil
	default:
		return nil, fmt.Errorf("compatibility line %q is invalid; require minimum, current, or all", line)
	}
}

func directRequirement(ctx context.Context, root, module string) (string, error) {
	content, err := capture(ctx, root, nil, "go", "mod", "edit", "-json")
	if err != nil {
		return "", fmt.Errorf("read direct module requirements: %w", err)
	}
	var metadata struct {
		Require []struct {
			Path     string
			Version  string
			Indirect bool
		}
	}
	if err := json.Unmarshal([]byte(content), &metadata); err != nil {
		return "", fmt.Errorf("decode go.mod metadata: %w", err)
	}
	for _, requirement := range metadata.Require {
		if requirement.Path == module && !requirement.Indirect && requirement.Version != "" {
			return requirement.Version, nil
		}
	}
	return "", fmt.Errorf("go.mod must directly require %s at an exact version", module)
}

func prepareCompatibilityDependencies(ctx context.Context, root, line string) error {
	versions, err := readCompatibility(root)
	if err != nil {
		return err
	}
	boundaries, err := versions.boundaries(line)
	if err != nil {
		return err
	}
	for _, boundary := range boundaries {
		if err := resolveExactCoreVersion(ctx, root, boundary.Version); err != nil {
			return fmt.Errorf("validate %s Spice version: %w", boundary.Name, err)
		}
		modfile, cleanup, err := alternateModfile(ctx, root, boundary.Version)
		if err != nil {
			return err
		}
		downloadErr := networkCommand(ctx, root, "mod", "download", "-modfile="+modfile)
		cleanup()
		if downloadErr != nil {
			return fmt.Errorf("prepare %s Spice graph: %w", boundary.Name, downloadErr)
		}
	}
	return nil
}

func resolveExactCoreVersion(ctx context.Context, root, version string) error {
	content, err := networkCapture(
		ctx,
		root,
		"list",
		"-mod=mod",
		"-m",
		"-json",
		spiceModulePath+"@"+version,
	)
	if err != nil {
		return err
	}
	var module struct {
		Path    string
		Version string
	}
	if err := json.Unmarshal([]byte(content), &module); err != nil {
		return fmt.Errorf("decode resolved Spice module: %w", err)
	}
	if module.Path != spiceModulePath || module.Version != version {
		return fmt.Errorf(
			"spice version resolved as %s@%s; require exactly %s@%s",
			module.Path, module.Version, spiceModulePath, version,
		)
	}
	return nil
}

func coreCompatibility(ctx context.Context, root, line string) error {
	versions, err := readCompatibility(root)
	if err != nil {
		return err
	}
	boundaries, err := versions.boundaries(line)
	if err != nil {
		return err
	}
	for _, boundary := range boundaries {
		if err := verifyCompatibilityBoundary(ctx, root, boundary); err != nil {
			return err
		}
	}
	return nil
}

func verifyCompatibilityBoundary(
	ctx context.Context,
	root string,
	boundary compatibilityBoundary,
) (returnErr error) {
	before, err := compatibilityState(root)
	if err != nil {
		return err
	}
	defer func() {
		after, stateErr := compatibilityState(root)
		if stateErr != nil {
			returnErr = errors.Join(returnErr, stateErr)
			return
		}
		if !maps.Equal(before, after) {
			returnErr = errors.Join(returnErr, errors.New("compatibility verification modified repository contents"))
		}
	}()

	modfile, cleanup, err := alternateModfile(ctx, root, boundary.Version)
	if err != nil {
		return err
	}
	defer cleanup()
	selected, err := capture(
		ctx,
		root,
		map[string]string{"GOFLAGS": "-mod=mod"},
		"go",
		"list",
		"-mod=mod",
		"-modfile="+modfile,
		"-m",
		"-f={{.Version}}",
		spiceModulePath,
	)
	if err != nil {
		return fmt.Errorf("resolve %s MVS graph: %w", boundary.Name, err)
	}
	if strings.TrimSpace(selected) != boundary.Version {
		return fmt.Errorf(
			"%s MVS graph selected Spice %q; require exactly %q",
			boundary.Name, strings.TrimSpace(selected), boundary.Version,
		)
	}
	packages, err := compatibilityPackages(ctx, root, modfile)
	if err != nil {
		return err
	}
	output.Printf(
		"testing %s Spice %s across %s",
		boundary.Name,
		boundary.Version,
		strings.Join(packages, ", "),
	)
	vetArguments := []string{"vet", "-mod=mod", "-modfile=" + modfile}
	vetArguments = append(vetArguments, packages...)
	if err := command(ctx, root, map[string]string{"GOFLAGS": "-mod=mod"}, "go", vetArguments...); err != nil {
		return err
	}
	testArguments := []string{
		"test",
		"-mod=mod",
		"-modfile=" + modfile,
		"-race",
		"-shuffle=on",
		"-count=1",
	}
	testArguments = append(testArguments, packages...)
	return command(ctx, root, map[string]string{"GOFLAGS": "-mod=mod"}, "go", testArguments...)
}

func compatibilityPackages(ctx context.Context, root, modfile string) ([]string, error) {
	content, err := capture(
		ctx,
		root,
		map[string]string{"GOFLAGS": "-mod=mod"},
		"go",
		"list",
		"-mod=mod",
		"-modfile="+modfile,
		"-f={{.ImportPath}}",
		"./...",
	)
	if err != nil {
		return nil, fmt.Errorf("list compatibility product packages: %w", err)
	}
	toolPackage := modulePath + "/internal/qualitygate"
	var result []string
	for candidate := range strings.FieldsSeq(content) {
		if candidate != toolPackage {
			result = append(result, candidate)
		}
	}
	slices.Sort(result)
	if len(result) == 0 {
		return nil, errors.New("compatibility graph contains no product packages")
	}
	return result, nil
}

func compatibilityState(root string) (map[string][sha256.Size]byte, error) {
	opened, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("open compatibility repository root: %w", err)
	}
	defer func() {
		if closeErr := opened.Close(); closeErr != nil {
			output.Printf("warning: close compatibility repository root %q: %v", root, closeErr)
		}
	}()
	result := make(map[string][sha256.Size]byte)
	err = fs.WalkDir(opened.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != "." && entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		content, readErr := opened.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("read compatibility state %q: %w", path, readErr)
		}
		result[filepath.ToSlash(path)] = sha256.Sum256(content)
		return nil
	})
	return result, err
}

func alternateModfile(ctx context.Context, root, spiceVersion string) (string, func(), error) {
	productMod, err := os.ReadFile(filepath.Join(root, "go.mod")) // #nosec G304 -- root is the fixed repository root.
	if err != nil {
		return "", nil, fmt.Errorf("read product go.mod: %w", err)
	}
	productSum, err := os.ReadFile(filepath.Join(root, "go.sum")) // #nosec G304 -- root is the fixed repository root.
	if err != nil {
		return "", nil, fmt.Errorf("read product go.sum: %w", err)
	}
	file, err := os.CreateTemp("", "spice-starter-kafka-compat-*.mod")
	if err != nil {
		return "", nil, fmt.Errorf("create compatibility modfile: %w", err)
	}
	modfile := file.Name()
	sumfile := strings.TrimSuffix(modfile, ".mod") + ".sum"
	cleanup := func() {
		for _, path := range []string{modfile, sumfile} {
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				output.Printf("warning: remove compatibility file %q: %v", path, removeErr)
			}
		}
	}
	if _, err := file.Write(productMod); err != nil {
		closeErr := file.Close()
		cleanup()
		return "", nil, errors.Join(
			fmt.Errorf("write compatibility modfile: %w", err),
			closeErr,
		)
	}
	if err := file.Close(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("close compatibility modfile: %w", err)
	}
	// #nosec G703 -- sumfile is derived only from the path returned by os.CreateTemp above.
	if err := os.WriteFile(sumfile, productSum, 0o600); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("write compatibility sumfile: %w", err)
	}
	if runErr := command(
		ctx,
		root,
		nil,
		"go",
		"mod",
		"edit",
		"-modfile="+modfile,
		"-require="+spiceModulePath+"@"+spiceVersion,
	); runErr != nil {
		cleanup()
		return "", nil, runErr
	}
	return modfile, cleanup, nil
}

func format(ctx context.Context, root string, write bool) error {
	files, err := goFiles(root)
	if err != nil {
		return err
	}
	for _, name := range []string{"goimports", "gofumpt"} {
		executable, pathErr := toolPath(ctx, root, name)
		if pathErr != nil {
			return pathErr
		}
		option := "-l"
		if write {
			option = "-w"
		}
		stdout, runErr := capture(ctx, root, nil, executable, append([]string{option}, files...)...)
		if runErr != nil {
			return runErr
		}
		if !write && strings.TrimSpace(stdout) != "" {
			return fmt.Errorf("%s requires formatting: %s", name, strings.Join(strings.Fields(stdout), ", "))
		}
	}
	return nil
}

func goFiles(root string) ([]string, error) {
	var result []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && path != root && slices.Contains([]string{".git", "tools", "vendor"}, entry.Name()) {
			return filepath.SkipDir
		}
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".go" {
			result = append(result, path)
		}
		return nil
	})
	slices.Sort(result)
	return result, err
}

func checkModule(ctx context.Context, root string) error {
	if err := command(ctx, root, nil, "go", "mod", "tidy", "-diff"); err != nil {
		return err
	}
	temporary, err := os.MkdirTemp("", "spice-starter-kafka-vendor-")
	if err != nil {
		return fmt.Errorf("create vendor comparison directory: %w", err)
	}
	defer removeTree(temporary)
	candidate := filepath.Join(temporary, "vendor")
	if vendorErr := command(ctx, root, nil, "go", "mod", "vendor", "-o", candidate); vendorErr != nil {
		return vendorErr
	}
	current, err := treeDigests(filepath.Join(root, "vendor"))
	if err != nil {
		return err
	}
	expected, err := treeDigests(candidate)
	if err != nil {
		return err
	}
	if !maps.Equal(current, expected) {
		return errors.New("vendor differs from a fresh go mod vendor result")
	}
	return nil
}

func removeTree(path string) {
	if err := os.RemoveAll(path); err != nil {
		output.Printf("warning: remove temporary tree %q: %v", path, err)
	}
}

func treeDigests(root string) (map[string][sha256.Size]byte, error) {
	opened, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("open vendor root: %w", err)
	}
	defer func() {
		if closeErr := opened.Close(); closeErr != nil {
			output.Printf("warning: close vendor root %q: %v", root, closeErr)
		}
	}()
	result := make(map[string][sha256.Size]byte)
	err = fs.WalkDir(opened.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		content, readErr := opened.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		result[filepath.ToSlash(path)] = sha256.Sum256(content)
		return nil
	})
	return result, err
}

func lint(ctx context.Context, root string) error {
	golangci, err := toolPath(ctx, root, "golangci-lint")
	if err != nil {
		return err
	}
	if lintErr := command(ctx, root, nil, golangci, "run", "--timeout=10m"); lintErr != nil {
		return lintErr
	}
	nilaway, err := toolPath(ctx, root, "nilaway")
	if err != nil {
		return err
	}
	return command(ctx, root, nil, nilaway, "-include-pkgs="+modulePath, "./...")
}

func security(ctx context.Context, root string) error {
	gosec, err := toolPath(ctx, root, "gosec")
	if err != nil {
		return err
	}
	if gosecErr := command(ctx, root, nil, gosec, "-quiet", "-exclude-generated", "./..."); gosecErr != nil {
		return gosecErr
	}
	govulncheck, err := toolPath(ctx, root, "govulncheck")
	if err != nil {
		return err
	}
	return command(ctx, root, nil, govulncheck, "./...")
}

func tests(ctx context.Context, root string) error {
	if err := command(ctx, root, nil, "go", "test", "-shuffle=on", "-count=1", "./..."); err != nil {
		return err
	}
	return command(ctx, root, nil, "go", "test", "-race", "-shuffle=on", "-count=1", "./...")
}

func coverage(ctx context.Context, root string) (returnErr error) {
	profile, err := os.CreateTemp("", "spice-starter-kafka-coverage-*.out")
	if err != nil {
		return fmt.Errorf("create coverage profile: %w", err)
	}
	path := profile.Name()
	if closeErr := profile.Close(); closeErr != nil {
		return fmt.Errorf("close coverage profile: %w", closeErr)
	}
	defer func() { returnErr = errors.Join(returnErr, os.Remove(path)) }()
	if coverageErr := command(ctx, root, nil, "go", "test", "-covermode=atomic", "-coverprofile="+path, "."); coverageErr != nil {
		return coverageErr
	}
	stdout, err := capture(ctx, root, nil, "go", "tool", "cover", "-func="+path)
	if err != nil {
		return err
	}
	percentage, err := totalCoverage(stdout)
	if err != nil {
		return err
	}
	output.Printf("coverage %.1f%% (minimum %.1f%%)", percentage, minimumCoverage)
	if percentage < minimumCoverage {
		return fmt.Errorf("coverage %.1f%% is below %.1f%%", percentage, minimumCoverage)
	}
	return nil
}

func totalCoverage(report string) (float64, error) {
	lines := strings.Split(strings.TrimSpace(report), "\n")
	if len(lines) == 0 {
		return 0, errors.New("coverage report is empty")
	}
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) == 0 || !strings.HasSuffix(fields[len(fields)-1], "%") {
		return 0, errors.New("coverage report has no total percentage")
	}
	value := strings.TrimSuffix(fields[len(fields)-1], "%")
	percentage, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("parse coverage percentage %q: %w", value, err)
	}
	return percentage, nil
}

func offline(ctx context.Context, root string) error {
	environment := map[string]string{"GOFLAGS": "-mod=vendor"}
	if err := command(ctx, root, environment, "go", "test", "-count=1", "./..."); err != nil {
		return err
	}
	return command(ctx, root, environment, "go", "build", "-trimpath", "./...")
}

func toolPath(ctx context.Context, root, name string) (string, error) {
	stdout, err := capture(ctx, root, nil, "go", "tool", "-C", "tools", "-n", name)
	if err != nil {
		return "", err
	}
	path := strings.TrimSpace(stdout)
	if path == "" {
		return "", fmt.Errorf("resolve tool %q: empty path", name)
	}
	return path, nil
}

func repositoryRoot() (string, error) {
	current, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for {
		content, readErr := os.ReadFile(filepath.Join(current, "go.mod")) // #nosec G304 -- candidates are bounded ancestors.
		if readErr == nil && bytes.Contains(content, []byte("module "+modulePath)) {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("find starter-kafka repository root: go.mod not found")
		}
		current = parent
	}
}

func command(ctx context.Context, directory string, environment map[string]string, executable string, arguments ...string) error {
	// #nosec G204,G702 -- executable and arguments are fixed repository-owned values.
	cmd := exec.CommandContext(ctx, executable, arguments...)
	cmd.Dir = directory
	cmd.Env = mergedEnvironment(environment)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", executable, strings.Join(arguments, " "), err)
	}
	return nil
}

func networkCommand(ctx context.Context, directory string, arguments ...string) error {
	// Dependency and module preparation is the sole network-capable verifier
	// phase. Go still authenticates every selected module against go.sum before
	// later checks run with GOPROXY=off.
	// #nosec G204,G702 -- executable and arguments are fixed repository-owned values.
	cmd := exec.CommandContext(ctx, "go", arguments...)
	cmd.Dir = directory
	cmd.Env = onlineEnvironment()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go %s: %w", strings.Join(arguments, " "), err)
	}
	return nil
}

func networkCapture(ctx context.Context, directory string, arguments ...string) (string, error) {
	// Compatibility preparation is explicitly network-capable. Go authenticates
	// the exact module through go.sum and the public checksum database before the
	// actual compatibility checks repeat with GOPROXY=off.
	// #nosec G204,G702 -- arguments are repository-owned values.
	cmd := exec.CommandContext(ctx, "go", arguments...)
	cmd.Dir = directory
	cmd.Env = onlineEnvironment()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("go %s: %w\n%s", strings.Join(arguments, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func capture(ctx context.Context, directory string, environment map[string]string, executable string, arguments ...string) (string, error) {
	// #nosec G204,G702 -- executable and arguments are fixed repository-owned values.
	cmd := exec.CommandContext(ctx, executable, arguments...)
	cmd.Dir = directory
	cmd.Env = mergedEnvironment(environment)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s %s: %w\n%s", executable, strings.Join(arguments, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func mergedEnvironment(overrides map[string]string) []string {
	values := map[string]string{"GOWORK": "off", "GOPROXY": "off", "GOFLAGS": "", "GOTOOLCHAIN": "local"}
	maps.Copy(values, overrides)
	result := make([]string, 0, len(os.Environ())+len(values))
	for _, entry := range os.Environ() {
		key, _, found := strings.Cut(entry, "=")
		if found {
			if _, replaced := values[strings.ToUpper(key)]; !replaced {
				result = append(result, entry)
			}
		}
	}
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	slices.Sort(result)
	return result
}

func onlineEnvironment() []string {
	values := map[string]string{"GOWORK": "off", "GOFLAGS": "", "GOTOOLCHAIN": "local"}
	result := make([]string, 0, len(os.Environ())+len(values))
	for _, entry := range os.Environ() {
		key, _, found := strings.Cut(entry, "=")
		if found {
			if _, replaced := values[strings.ToUpper(key)]; !replaced {
				result = append(result, entry)
			}
		}
	}
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	slices.Sort(result)
	return result
}
