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
				"f8fe9ec3cedd17f8bec4bf3d40f6640902774124",
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
			name:    "forwarded secrets",
			content: strings.Replace(valid, "    with:\n", "    secrets: inherit\n    with:\n", 1),
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
