// Package release builds deterministic, signed starter-kafka source releases.
package release

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

const maxGitDiagnosticBytes = 32 << 10

var canonicalSemver = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)

// Config describes one guarded source release build.
type Config struct {
	Root          string
	OutputDir     string
	Version       string
	Epoch         time.Time
	PrivateKey    []byte
	AllowUnsigned bool
}

// Result describes the atomically committed release directory.
type Result struct {
	OutputDir string
	Files     []string
}

// Build creates release artifacts without overwriting an existing path or
// consulting the network. Callers own exact-tag and checkout validation.
func Build(ctx context.Context, config Config) (result Result, resultErr error) {
	normalized, err := normalizeConfig(ctx, config)
	if err != nil {
		return Result{}, err
	}
	parent := filepath.Dir(normalized.OutputDir)
	if mkdirErr := os.MkdirAll(parent, 0o750); mkdirErr != nil {
		return Result{}, fmt.Errorf("create release parent directory: %w", mkdirErr)
	}
	staging, err := os.MkdirTemp(parent, ".starter-kafka-release-*")
	if err != nil {
		return Result{}, fmt.Errorf("create release staging directory: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			resultErr = errors.Join(resultErr, os.RemoveAll(staging))
		}
	}()
	files, err := buildArtifacts(ctx, normalized, staging)
	if err != nil {
		return Result{}, err
	}
	if err := os.Rename(staging, normalized.OutputDir); err != nil {
		return Result{}, fmt.Errorf("commit release directory %q: %w", normalized.OutputDir, err)
	}
	committed = true
	return Result{OutputDir: normalized.OutputDir, Files: files}, nil
}

func normalizeConfig(ctx context.Context, config Config) (Config, error) {
	if ctx == nil {
		return Config{}, fmt.Errorf("build release: context is nil")
	}
	if !isCanonicalSemver(config.Version) {
		return Config{}, fmt.Errorf("build release: version %q is not canonical semantic version", config.Version)
	}
	if config.Epoch.IsZero() {
		return Config{}, fmt.Errorf("build release: source epoch is required")
	}
	if len(config.PrivateKey) == 0 && !config.AllowUnsigned {
		return Config{}, fmt.Errorf("build release: Ed25519 signing key is required unless unsigned rehearsal is explicit")
	}
	if len(config.PrivateKey) != 0 && config.AllowUnsigned {
		return Config{}, fmt.Errorf("build release: unsigned rehearsal must not receive a signing key")
	}
	root, err := filepath.Abs(config.Root)
	if err != nil {
		return Config{}, fmt.Errorf("resolve release root: %w", err)
	}
	output, err := filepath.Abs(config.OutputDir)
	if err != nil {
		return Config{}, fmt.Errorf("resolve release output directory: %w", err)
	}
	if _, err := os.Stat(output); err == nil {
		return Config{}, fmt.Errorf("build release: output directory %q already exists", output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Config{}, fmt.Errorf("inspect release output directory: %w", err)
	}
	for _, name := range []string{"go.mod", "go.sum", "LICENSE", "README.md", "vendor/modules.txt"} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			return Config{}, fmt.Errorf("build release: required root file %q: %w", name, err)
		}
	}
	config.Root = root
	config.OutputDir = output
	config.Epoch = config.Epoch.UTC().Truncate(time.Second)
	config.PrivateKey = append([]byte(nil), config.PrivateKey...)
	return config, nil
}

func isCanonicalSemver(version string) bool {
	if !canonicalSemver.MatchString(version) {
		return false
	}
	withoutBuild, _, _ := strings.Cut(version, "+")
	_, prerelease, found := strings.Cut(withoutBuild, "-")
	if !found {
		return true
	}
	for identifier := range strings.SplitSeq(prerelease, ".") {
		if len(identifier) > 1 && identifier[0] == '0' {
			numeric := true
			for _, character := range identifier {
				if character < '0' || character > '9' {
					numeric = false
					break
				}
			}
			if numeric {
				return false
			}
		}
	}
	return true
}

func buildArtifacts(ctx context.Context, config Config, staging string) ([]string, error) {
	entries, err := sourceEntries(ctx, config.Root, config.Version)
	if err != nil {
		return nil, err
	}
	versionName := strings.TrimPrefix(config.Version, "v")
	archiveName := "starter-kafka_" + versionName + "_source.tar.gz"
	if archiveErr := writeSourceArchive(filepath.Join(staging, archiveName), config.Epoch, entries); archiveErr != nil {
		return nil, archiveErr
	}
	sbomName := "starter-kafka_" + versionName + "_sbom.spdx.json"
	sbom, err := buildSBOM(ctx, config.Root, config.Version, config.Epoch)
	if err != nil {
		return nil, err
	}
	if writeErr := writeNewFile(filepath.Join(staging, sbomName), sbom); writeErr != nil {
		return nil, writeErr
	}
	files := []string{archiveName, sbomName}
	slices.Sort(files)
	checksums, err := artifactChecksums(staging, files)
	if err != nil {
		return nil, err
	}
	if err := writeNewFile(filepath.Join(staging, "checksums.txt"), checksums); err != nil {
		return nil, err
	}
	files = append(files, "checksums.txt")
	if len(config.PrivateKey) != 0 {
		signature, publicKey, err := signChecksums(checksums, config.PrivateKey)
		if err != nil {
			return nil, err
		}
		if err := writeNewFile(filepath.Join(staging, "checksums.txt.sig"), signature); err != nil {
			return nil, err
		}
		if err := writeNewFile(filepath.Join(staging, "checksums.txt.pem"), publicKey); err != nil {
			return nil, err
		}
		files = append(files, "checksums.txt.pem", "checksums.txt.sig")
	}
	slices.Sort(files)
	return files, nil
}

func sourceEntries(ctx context.Context, root, version string) ([]archiveEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("list release sources: %w", err)
	}
	// Git is only the committed source index here; archive construction remains
	// repository-owned and never delegates dependency or artifact resolution.
	command := exec.CommandContext(ctx, "git", "ls-files", "-z") // #nosec G204 -- fixed executable and arguments.
	command.Dir = root
	command.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("list release sources: %w: %s", err, boundedText(stderr.Bytes()))
	}
	names := strings.Split(strings.TrimSuffix(stdout.String(), "\x00"), "\x00")
	if len(names) == 1 && names[0] == "" {
		return nil, fmt.Errorf("list release sources: repository has no tracked files")
	}
	slices.Sort(names)
	prefix := "starter-kafka-" + strings.TrimPrefix(version, "v")
	entries := make([]archiveEntry, 0, len(names))
	for _, name := range names {
		clean := path.Clean(filepath.ToSlash(name))
		if clean == "." || clean != filepath.ToSlash(name) || path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
			return nil, fmt.Errorf("list release sources: unsafe tracked path %q", name)
		}
		data, err := readScopedFile(root, clean)
		if err != nil {
			return nil, fmt.Errorf("read tracked release source %q: %w", clean, err)
		}
		entries = append(entries, archiveEntry{name: prefix + "/" + clean, data: data})
	}
	return entries, nil
}

func artifactChecksums(root string, files []string) ([]byte, error) {
	var result strings.Builder
	for _, name := range files {
		data, err := readScopedFile(root, name)
		if err != nil {
			return nil, fmt.Errorf("read release artifact %q: %w", name, err)
		}
		sum := sha256.Sum256(data)
		fmt.Fprintf(&result, "%s  %s\n", hex.EncodeToString(sum[:]), name)
	}
	return []byte(result.String()), nil
}

func writeNewFile(filename string, data []byte) error {
	root, err := os.OpenRoot(filepath.Dir(filename))
	if err != nil {
		return fmt.Errorf("open release artifact root: %w", err)
	}
	file, err := root.OpenFile(filepath.Base(filename), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errors.Join(fmt.Errorf("create release artifact %q: %w", filename, err), root.Close())
	}
	_, writeErr := file.Write(data)
	return errors.Join(writeErr, file.Close(), root.Close())
}

func readScopedFile(rootPath, name string) ([]byte, error) {
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf("open scoped root %q: %w", rootPath, err)
	}
	file, err := root.Open(filepath.ToSlash(name))
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	data, readErr := io.ReadAll(file)
	return data, errors.Join(readErr, file.Close(), root.Close())
}

func boundedText(data []byte) string {
	if len(data) > maxGitDiagnosticBytes {
		data = data[len(data)-maxGitDiagnosticBytes:]
	}
	return strings.TrimSpace(string(data))
}
