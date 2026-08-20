# Release Please Initialization

## Overview

This pull request initializes [`release-please`](https://github.com/googleapis/release-please) for automated semantic versioning, changelog generation, and GitHub Releases in `eventsalsa/worker`.

## Key Changes

### 1. Release Please Manifest Mode Configuration
- [`.release-please-manifest.json`](.release-please-manifest.json): Initialized with the current package version (`0.0.3`).
- [`release-please-config.json`](release-please-config.json): Configured for Go package release type with `bump-minor-pre-major` enabled, changelog tracking at `CHANGELOG.md`, and official schema validation.

### 2. Unified Single-Workflow Architecture
- [`.github/workflows/release.yml`](.github/workflows/release.yml):
  - Configured with `contents: write` and `pull-requests: write` permissions.
  - Implements the single-workflow pattern where `googleapis/release-please-action` executes first (`id: release`), followed conditionally by checkout and Go setup steps when a release is cut (`if: ${{ steps.release.outputs.release_created }}`).
  - Action steps are pinned to verified, exact 40-character commit SHAs with inline tag comments:
    - `googleapis/release-please-action@45996ed1f6d02564a971a2fa1b5860e934307cf7` (`# v5.0.0`)
    - `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1` (`# v7.0.1`)
    - `actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e` (`# v7.0.0`)

## Verification

- **Syntax & Schema Validation**: Verified JSON and YAML configuration syntax.
- **Repository Quality Checks**: Ran `rtk make check` (linter, unit tests with race detector, integration tests against PostgreSQL 16 container); all checks passed successfully.
