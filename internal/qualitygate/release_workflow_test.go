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
				"164e81ea4a31fa124670dc69afaec5bdf5747d78",
				strings.Repeat("0", 40),
				1,
			),
		},
		{
			name:    "wrong module",
			content: strings.Replace(valid, modulePath, "github.com/spice-framework/wrong", 1),
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
