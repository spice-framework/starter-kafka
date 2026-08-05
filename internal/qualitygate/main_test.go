package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadCompatibility(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name: "valid",
			content: `{
				"schema": 1,
				"minimum": "v0.0.0-20260101000000-111111111111",
				"current": "v0.0.0-20260201000000-222222222222"
			}`,
		},
		{
			name:    "unknown field",
			content: `{"schema":1,"minimum":"a","current":"b","future":true}`,
			wantErr: "unknown field",
		},
		{
			name:    "trailing value",
			content: `{"schema":1,"minimum":"a","current":"b"} {}`,
			wantErr: "trailing JSON values",
		},
		{
			name:    "unsupported schema",
			content: `{"schema":2,"minimum":"a","current":"b"}`,
			wantErr: "schema 2 is unsupported",
		},
		{
			name:    "missing boundary",
			content: `{"schema":1,"minimum":"a","current":" "}`,
			wantErr: "requires explicit minimum and current versions",
		},
		{
			name:    "identical boundaries",
			content: `{"schema":1,"minimum":"a","current":"a"}`,
			wantErr: "minimum and current versions must differ",
		},
		{
			name:    "surrounding whitespace",
			content: `{"schema":1,"minimum":" a","current":"b"}`,
			wantErr: "versions must not contain surrounding whitespace",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			if err := os.WriteFile(
				filepath.Join(root, compatibilityFile),
				[]byte(test.content),
				0o600,
			); err != nil {
				t.Fatalf("write compatibility fixture: %v", err)
			}
			got, err := readCompatibility(root)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("readCompatibility() error = %v", err)
				}
				if got.Minimum == "" || got.Current == "" {
					t.Fatalf("readCompatibility() = %#v, want both boundaries", got)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("readCompatibility() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestCompatibilityBoundaries(t *testing.T) {
	t.Parallel()
	versions := compatibilityVersions{Minimum: "minimum-version", Current: "current-version"}
	tests := []struct {
		line    string
		want    []compatibilityBoundary
		wantErr string
	}{
		{line: "minimum", want: []compatibilityBoundary{{Name: "minimum", Version: versions.Minimum}}},
		{line: "current", want: []compatibilityBoundary{{Name: "current", Version: versions.Current}}},
		{
			line: "all",
			want: []compatibilityBoundary{
				{Name: "minimum", Version: versions.Minimum},
				{Name: "current", Version: versions.Current},
			},
		},
		{line: "latest", wantErr: "require minimum, current, or all"},
	}
	for _, test := range tests {
		t.Run(test.line, func(t *testing.T) {
			t.Parallel()
			got, err := versions.boundaries(test.line)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("boundaries(%q) error = %v, want containing %q", test.line, err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("boundaries(%q) error = %v", test.line, err)
			}
			if len(got) != len(test.want) {
				t.Fatalf("boundaries(%q) = %#v, want %#v", test.line, got, test.want)
			}
			for index := range test.want {
				if got[index] != test.want[index] {
					t.Fatalf("boundaries(%q) = %#v, want %#v", test.line, got, test.want)
				}
			}
		})
	}
}

func TestDirectRequirement(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		want    string
		wantErr string
	}{
		{
			name: "direct",
			content: "module example.com/test\n\ngo 1.26.0\n\n" +
				"require github.com/spice-framework/spice v0.1.0\n",
			want: "v0.1.0",
		},
		{
			name: "indirect",
			content: "module example.com/test\n\ngo 1.26.0\n\n" +
				"require github.com/spice-framework/spice v0.1.0 // indirect\n",
			wantErr: "must directly require",
		},
		{
			name:    "absent",
			content: "module example.com/test\n\ngo 1.26.0\n",
			wantErr: "must directly require",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(test.content), 0o600); err != nil {
				t.Fatalf("write go.mod fixture: %v", err)
			}
			got, err := directRequirement(t.Context(), root, spiceModulePath)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("directRequirement() error = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("directRequirement() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("directRequirement() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestCompatibilityStateTracksRepositoryAndIgnoresGitMetadata(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, directory := range []string{".git", "vendor/example.com/dependency"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o750); err != nil {
			t.Fatalf("create fixture directory %q: %v", directory, err)
		}
	}
	fixtures := map[string]string{
		"kafka.go":                               "package kafka\n",
		"vendor/example.com/dependency/value.go": "package dependency\n",
		".git/index":                             "ignored metadata",
	}
	for name, content := range fixtures {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte(content), 0o600); err != nil {
			t.Fatalf("write fixture %q: %v", name, err)
		}
	}
	before, err := compatibilityState(root)
	if err != nil {
		t.Fatalf("compatibilityState() error = %v", err)
	}
	if _, exists := before[".git/index"]; exists {
		t.Fatal("compatibilityState() included Git metadata")
	}
	if _, exists := before["vendor/example.com/dependency/value.go"]; !exists {
		t.Fatal("compatibilityState() omitted vendor content")
	}
	if writeErr := os.WriteFile(filepath.Join(root, "kafka.go"), []byte("package changed\n"), 0o600); writeErr != nil {
		t.Fatalf("modify source fixture: %v", writeErr)
	}
	after, err := compatibilityState(root)
	if err != nil {
		t.Fatalf("compatibilityState() after modification error = %v", err)
	}
	if before["kafka.go"] == after["kafka.go"] {
		t.Fatal("compatibilityState() did not detect source modification")
	}
}
