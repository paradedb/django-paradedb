# Release Management & Compatibility

This document defines release policies for `django-paradedb`, including
versioning, compatibility promises, and support windows. It is intentionally
lightweight while ParadeDB is pre-1.0.

## Goals

- Keep Django integration stable and predictable.
- Track ParadeDB capabilities without blocking users on upgrades.
- Minimize maintenance burden while providing a clear support matrix.

## Current Status (Pre-1.0 ParadeDB)

ParadeDB has not reached 1.0.0. Until it does:

- We treat ParadeDB minor versions as potentially breaking.
- We will document feature availability by ParadeDB version.
- We will prefer capability-based gating for new features when feasible.

## Versioning Policy (Library)

We follow SemVer for `django-paradedb`:

- **MAJOR**: Breaking changes in the Django integration API or behavior.
- **MINOR**: New features or non-breaking behavior additions.
- **PATCH**: Bug fixes and documentation changes only.

## Compatibility Principles

1. **Major Alignment (Optional Post-1.0)**
   - When ParadeDB reaches 1.0, consider aligning major versions
     (e.g., ParadeDB 1.x ↔ django-paradedb 1.x).
2. **Forward-Compatible Minor Policy**
   - A given library minor should work with the same ParadeDB major and
     later minor versions, but new ParadeDB features require a newer
     library minor to use.
3. **Capability Gating**
   - If a ParadeDB feature is version-gated, the library should expose a
     predictable API and raise a clear error (or no-op) when unsupported.

## Support Matrix (Draft)

This matrix is a _policy target_ and should be updated as the project grows.

- **Django**: Current LTS + previous LTS
- **ParadeDB**: Latest minor + previous minor (while pre-1.0)
- **Postgres**: ParadeDB-supported versions only
- **Python**: Match Django LTS requirements

## Release Cadence

- **Regular releases**: As needed; bundled feature releases are preferred.
- **Hotfix releases**: Patch-only for regressions or critical bugs.

## Deprecation Policy

- Deprecations must be documented with the version they were introduced.
- Remove deprecated behavior no earlier than the next MINOR release, or
  after two MINOR releases, whichever is later.

## Feature Availability

Features that depend on ParadeDB capabilities should be documented here,
or in `README.md`, with their minimum ParadeDB version.

Example format:

- `feature_name`: ParadeDB >= 0.x.y

## Testing & CI

We aim to test the support matrix in CI:

- Django: current LTS + previous LTS
- ParadeDB: latest minor + previous minor
- Postgres: ParadeDB-supported versions

Non-blocking (optional):

- ParadeDB nightly/pre-release builds

## Open Decisions

The following items should be decided once ParadeDB 1.0.0 ships:

- Do we align `django-paradedb` major versions to ParadeDB major versions?
- What is the long-term support window for older ParadeDB majors?
- Do we want a separate "compatibility table" in `README.md`?
