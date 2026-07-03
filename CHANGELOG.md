# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.4.0 - 2026-07-03

### Added

- ability to ignore/unignore topic(s)
- full url generation for sharing the dashboard with all the states
- copy to clipboard helper

## v0.3.3 - 2026-06-19

### Fixed

- CVE-2026-40898 quic-go

### Added

- add "Has Unused" badge

### Changed

- split empty topic/partitions as Empty and Has Empty
- split stale topic as Stale and Has Stale

## v0.3.2 - 2026-05-18

### Fixed

- page selector was not working

### Added

- ability to show also partialy empty topics if it has one or more empty partitions

### Changed

- tracker topic shards are now assigned based on murmur2 hash, aligns with producer partitioner

## v0.3.1 - 2026-05-15

### Fixed

- potential architecture dependent int parsing
- logging sensitive information

### Changed

- Tracker now scans immediately at startup then follows RIPT_SCAN_INTERVAL_MINUTES
- the configuration STATIC_FILES_DIR > HTTP_STATIC_FILES_DIR

## v0.3.0 - 2026-04-30

### Added

- /api/empty for listing empty topics
- Ability to show empty topics on Dashboard
- message counts for topic/partitions

### Changed

- Topic State Record schema now includes is_empty field,  

## v0.2.0 - 2026-04-23

### Added

- RIPT now can also run as a cli utility

### Fixed

- Handling empty assignments
- Empty state topic, bootstrap guard
- Kafka clientSoftwareName and Version not propogating with all requests ([bug at upstream franz-go module](https://github.com/twmb/franz-go/issues/1296))

### Changed

- Upgraded franz-go v1.21.0

## v0.1.0 - 2026-04-16

### Added

- Kafka clientSoftwareName and Version required by the KIP-714 with a caveat see Known Issues below

### Fixed

- Timestamps were resetting when multiple instances of RIPT are running

### Changed

- Upgraded Go 1.26
- Upgraded franz-go v1.20.7
- Upgraded Gin v1.12.0
- Stale partitions in topic detail modal are now colored as yellow, consistent with table view

### Known Issues

- SoftwareNameAndVersion not propagated to internal group coordinator connection ([bug at franz-go module](https://github.com/twmb/franz-go/issues/1296))

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
