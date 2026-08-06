package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const reusableReleaseWorkflow = "spice-framework/.github/.github/workflows/library-release.yml@164e81ea4a31fa124670dc69afaec5bdf5747d78"

func checkIdentityAndReleaseWorkflow(ctx context.Context, root string) error {
	if err := checkIdentity(ctx, root); err != nil {
		return err
	}
	return validateReleaseWorkflowFile(root)
}

func validateReleaseWorkflowFile(root string) error {
	content, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "release.yml")) // #nosec G304 -- root is the fixed repository root.
	if err != nil {
		return fmt.Errorf("read release workflow: %w", err)
	}
	return validateReleaseWorkflow(content)
}

func validateReleaseWorkflow(content []byte) error {
	normalized := strings.ReplaceAll(string(content), "\r\n", "\n")
	if normalized != expectedReleaseWorkflow() {
		return errors.New("release workflow must be the exact least-privilege central caller with pinned workflow and module")
	}
	return nil
}

func expectedReleaseWorkflow() string {
	return fmt.Sprintf(`name: Release

on:
  push:
    tags:
      - "v[0-9]*.[0-9]*.[0-9]*"

permissions: {}

concurrency:
  group: release-${{ github.ref }}
  cancel-in-progress: false

jobs:
  release:
    name: Verify, sign, and publish
    permissions:
      contents: write
    uses: %s
    with:
      module: %s
`, reusableReleaseWorkflow, modulePath)
}
