package release

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestBuildProducesByteIdenticalSignedSourceRelease(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	epoch := time.Unix(1_788_000_000, 0).UTC()
	seed := make([]byte, ed25519.SeedSize)
	for index := range seed {
		seed[index] = byte(index + 1)
	}
	parent := t.TempDir()
	config := Config{
		Root: root, OutputDir: filepath.Join(parent, "first"), Version: "v0.1.0-rc.1", Epoch: epoch,
		PrivateKey: []byte(base64.StdEncoding.EncodeToString(seed)),
	}
	first, err := Build(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	config.OutputDir = filepath.Join(parent, "second")
	second, err := Build(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first.Files, second.Files) || len(first.Files) != 5 {
		t.Fatalf("release files differ: %v != %v", first.Files, second.Files)
	}
	for _, name := range first.Files {
		left, readErr := os.ReadFile(filepath.Join(first.OutputDir, name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		right, readErr := os.ReadFile(filepath.Join(second.OutputDir, name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !slices.Equal(left, right) {
			t.Errorf("release artifact %q is not byte reproducible", name)
		}
	}
	verifySignedChecksums(t, first.OutputDir)
	verifySourceArchive(t, first.OutputDir, root, epoch)
	verifySBOM(t, first.OutputDir, root, epoch)
	if _, err := Build(t.Context(), config); err == nil {
		t.Fatal("Build() overwrote an existing output directory")
	}
}

func TestBuildRejectsUnsafeOrIncompleteConfiguration(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	base := Config{Root: root, OutputDir: filepath.Join(t.TempDir(), "release"), Version: "v0.1.0", Epoch: time.Unix(1, 0), AllowUnsigned: true}
	tests := []struct {
		name   string
		change func(*Config)
		nilCtx bool
	}{
		{name: "nil context", nilCtx: true},
		{name: "invalid version", change: func(config *Config) { config.Version = "v01.0.0" }},
		{name: "noncanonical prerelease", change: func(config *Config) { config.Version = "v1.0.0-01" }},
		{name: "missing epoch", change: func(config *Config) { config.Epoch = time.Time{} }},
		{name: "unsigned not explicit", change: func(config *Config) { config.AllowUnsigned = false }},
		{name: "signed rehearsal", change: func(config *Config) { config.PrivateKey = []byte("key") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := base
			config.OutputDir += "-" + strings.ReplaceAll(test.name, " ", "-")
			if test.change != nil {
				test.change(&config)
			}
			ctx := t.Context()
			if test.nilCtx {
				ctx = nil
			}
			if _, err := Build(ctx, config); err == nil {
				t.Fatal("Build() error = nil")
			}
		})
	}
}

func TestModuleGraphRejectsVendorDriftAndCancellation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFixtureFile(t, root, "go.mod", "module example.com/starter\n\ngo 1.26.0\n\nrequire example.com/dependency v1.2.3\n")
	writeFixtureFile(t, root, "vendor/modules.txt", "# example.com/dependency v1.2.4\n## explicit; go 1.26\n")
	if _, err := listModules(t.Context(), root); err == nil {
		t.Fatal("vendor drift was accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := listModules(ctx, root); err == nil {
		t.Fatal("cancelled module read was accepted")
	}
}

func TestSigningRejectsInvalidKeys(t *testing.T) {
	t.Parallel()
	for _, key := range [][]byte{[]byte("%%%"), []byte(base64.StdEncoding.EncodeToString([]byte("short")))} {
		if _, _, err := signChecksums([]byte("checksums"), key); err == nil {
			t.Fatal("invalid signing key was accepted")
		}
	}
}

func verifySignedChecksums(t *testing.T, directory string) {
	t.Helper()
	checksums := readTestFile(t, filepath.Join(directory, "checksums.txt"))
	signature := readTestFile(t, filepath.Join(directory, "checksums.txt.sig"))
	publicPEM := readTestFile(t, filepath.Join(directory, "checksums.txt.pem"))
	block, _ := pem.Decode(publicPEM)
	if block == nil {
		t.Fatal("public key is not PEM")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, ok := parsed.(ed25519.PublicKey)
	if !ok || !ed25519.Verify(publicKey, checksums, signature) {
		t.Fatal("release checksum signature did not verify")
	}
	tampered := append([]byte(nil), checksums...)
	tampered[0] ^= 1
	if ed25519.Verify(publicKey, tampered, signature) {
		t.Fatal("signature accepted tampered checksums")
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(checksums)), "\n") {
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) != 2 {
			t.Fatalf("invalid checksum line %q", line)
		}
		data := readTestFile(t, filepath.Join(directory, parts[1]))
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != parts[0] {
			t.Fatalf("checksum mismatch for %q", parts[1])
		}
	}
}

func verifySourceArchive(t *testing.T, directory, root string, epoch time.Time) {
	t.Helper()
	archivePath := filepath.Join(directory, "starter-kafka_0.1.0-rc.1_source.tar.gz")
	file, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("close source archive: %v", closeErr)
		}
	}()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := gzipReader.Close(); closeErr != nil {
			t.Errorf("close gzip reader: %v", closeErr)
		}
	}()
	found := map[string]bool{}
	reader := tar.NewReader(gzipReader)
	for {
		header, nextErr := reader.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if !header.ModTime.Equal(epoch) || !header.AccessTime.Equal(epoch) || !header.ChangeTime.Equal(epoch) {
			t.Fatalf("entry %q has nondeterministic timestamps", header.Name)
		}
		if !strings.HasPrefix(header.Name, "starter-kafka-0.1.0-rc.1/") || filepath.IsAbs(header.Name) {
			t.Fatalf("unsafe archive path %q", header.Name)
		}
		found[strings.TrimPrefix(header.Name, "starter-kafka-0.1.0-rc.1/")] = true
	}
	for _, name := range []string{"go.mod", "go.sum", "vendor/modules.txt", "README.md", "LICENSE"} {
		if !found[name] {
			t.Errorf("source archive is missing %s", name)
		}
	}
	data := readTestFile(t, archivePath)
	if bytes.Contains(data, []byte(root)) || bytes.Contains(data, []byte(filepath.ToSlash(root))) {
		t.Fatal("source archive contains an absolute workspace path")
	}
}

func verifySBOM(t *testing.T, directory, root string, epoch time.Time) {
	t.Helper()
	data := readTestFile(t, filepath.Join(directory, "starter-kafka_0.1.0-rc.1_sbom.spdx.json"))
	var document spdxDocument
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	if document.SPDXVersion != "SPDX-2.3" || document.CreationInfo.Created != epoch.Format(time.RFC3339) || len(document.Packages) < 2 || len(document.Relationships) != len(document.Packages)-1 {
		t.Fatalf("SBOM = %#v", document)
	}
	if strings.Contains(string(data), root) || strings.Contains(string(data), filepath.ToSlash(root)) {
		t.Fatal("SBOM contains an absolute workspace path")
	}
}

func writeFixtureFile(t *testing.T, root, name, content string) {
	t.Helper()
	filename := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(filename), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readTestFile(t *testing.T, filename string) []byte {
	t.Helper()
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
