package main

import (
	"strings"
	"testing"
)

func TestValidateReleaseWorkflow(t *testing.T) {
	t.Parallel()
	valid := expectedReleaseWorkflow()
	tests := []struct {
		name    string
		content string
	}{
		{name: "exact caller", content: valid},
		{
			name: "wrong reusable workflow commit",
			content: strings.Replace(
				valid,
				"8b9fc5012de2f2e457ff13d3f1168a451da167fe",
				strings.Repeat("0", 40),
				1,
			),
		},
		{
			name:    "wrong module",
			content: strings.Replace(valid, modulePath, "github.com/spice-framework/wrong", 1),
		},
		{
			name:    "broad top-level permissions",
			content: strings.Replace(valid, "permissions: {}", "permissions:\n  contents: write", 1),
		},
		{
			name:    "missing job-local publish permission",
			content: strings.Replace(valid, "    permissions:\n      contents: write\n", "", 1),
		},
		{
			name:    "missing named signing secret",
			content: strings.Replace(valid, "    secrets:\n      SPICE_LIBRARY_RELEASE_SIGNING_KEY: ${{ secrets.SPICE_LIBRARY_RELEASE_SIGNING_KEY }}\n", "", 1),
		},
		{
			name:    "inherited unrelated secrets",
			content: strings.Replace(valid, "    secrets:\n      SPICE_LIBRARY_RELEASE_SIGNING_KEY: ${{ secrets.SPICE_LIBRARY_RELEASE_SIGNING_KEY }}", "    secrets: inherit", 1),
		},
		{
			name:    "wrong signing secret",
			content: strings.Replace(valid, "secrets.SPICE_LIBRARY_RELEASE_SIGNING_KEY", "secrets.WRONG", 1),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateReleaseWorkflow([]byte(test.content))
			if test.name == "exact caller" {
				if err != nil {
					t.Fatalf("validateReleaseWorkflow() error = %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("validateReleaseWorkflow() error = nil")
			}
		})
	}
}
