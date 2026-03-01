# Release Management and Compatibility

This document describes how `django-paradedb` versions are released and what
compatibility guarantees the project provides.

## Goals

- Keep the Django integration stable and predictable.
- Add ParadeDB features quickly without forcing unnecessary upgrades.
- Keep support expectations clear for users and maintainers.

## Current Status (ParadeDB Pre-1.0)

ParadeDB is still pre-1.0, so minor releases may include breaking changes.
During this phase:

- Feature availability is documented by ParadeDB version.
- New functionality should use capability checks when practical.
- Errors for unsupported capabilities should be explicit.

## Versioning Policy

`django-paradedb` follows SemVer:

- **MAJOR**: Breaking API or behavior changes.
- **MINOR**: New features and non-breaking enhancements.
- **PATCH**: Bug fixes and documentation updates.

## Compatibility Principles

1. Major-version alignment after ParadeDB 1.0: we plan to align majors where
   practical (for example, ParadeDB 1.x with `django-paradedb` 1.x).
2. Minor forward compatibility: a given library minor should support the same
   ParadeDB major and later ParadeDB minors.
3. Capability gating: if a feature depends on a ParadeDB version, expose a
   stable API and return a clear error (or no-op) when unsupported.

## Support Matrix

- **Django**: Current LTS and previous LTS.
- **ParadeDB**: Latest minor and previous minor (while ParadeDB is pre-1.0).
- **Postgres**: Versions supported by ParadeDB.
- **Python**: Versions required by supported Django releases.

Update this matrix whenever support policy changes.

## Release Cadence

- **Regular releases**: As needed, typically bundled feature updates.
- **Hotfix releases**: Patch releases for regressions or critical bugs.

## Deprecation Policy

- Every deprecation must note the version where it was introduced.
- Remove deprecated behavior no earlier than the next MINOR release, or after
  two MINOR releases, whichever is later.

## Feature Availability Documentation

Document ParadeDB version requirements for version-gated features in this file
or in `README.md`.

Use this format:

- `feature_name`: ParadeDB >= `0.x.y`

## Testing and CI Expectations

CI should cover the published support matrix:

- Django: current LTS and previous LTS.
- ParadeDB: latest minor and previous minor.
- Postgres: ParadeDB-supported versions.

Optional non-blocking jobs may include ParadeDB nightly or pre-release builds.

## Decisions for ParadeDB 1.0

Revisit these points once ParadeDB reaches 1.0:

- Final major-version alignment policy.
- Long-term support window for older ParadeDB majors.
- Whether to duplicate the support matrix in `README.md`.
