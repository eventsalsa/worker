# Changelog

## [0.2.0](https://github.com/eventsalsa/projector/compare/v0.1.0...v0.2.0) (2026-08-21)


### Features

* **projector:** rename worker to projector and upgrade to store v0.2.0 ([#13](https://github.com/eventsalsa/projector/issues/13)) ([312f2fe](https://github.com/eventsalsa/projector/commit/312f2fe87d6717ad86f01cd7c34f9580fe4b40ac))

## [0.1.0](https://github.com/eventsalsa/worker/compare/v0.0.3...v0.1.0) (2026-08-20)


### ⚠ BREAKING CHANGES

* **worker:** replace aggregate terminology with stream across consumer contracts and event filtering

### Code Refactoring

* **worker:** align with stream terminology and upgrade store to v0.1.0 ([#11](https://github.com/eventsalsa/worker/issues/11)) ([e07b3c9](https://github.com/eventsalsa/worker/commit/e07b3c93f5e389b074a721b169a9d67119cacb9e))

## v0.0.2

### Added

- Added `cmd/migrate-gen`, a stable CLI entrypoint for generating worker infrastructure migrations with the same defaults and table-name overrides as `migrations.Config`.

### Documentation

- Documented the quick CLI flow and the package-level migration API in `README.md` and `migrations` package docs.
