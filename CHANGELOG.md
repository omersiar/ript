# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Kafka clientSoftwareName and Version

## v0.0.2 - 2026-04-11

### Added

- Clear (×) button on the topic search input
- Direct page navigation
- Prevent scans from running while snapshots are being saved
- Configurable static files directory for the web dashboard

### Changed

- Cleaned up and reduced code across API, tracker, state manager, and models
- Snapshots are now saved more efficiently

### Removed

- Test harness and testing code from main app
- Various dead code and unnecessary nil checks

### Fixed

- Graceful shutdown when broker connection is lost
- Default topic creation timeout
- Overlapping is-invalid regex validation indicator

## v0.0.1 - 2026-04-04

### Added

- Initial version
