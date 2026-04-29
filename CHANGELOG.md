# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

RIPT only cares about the offsets so if it find partitions' earliest and latest offsets are the same it marks them as empty

It is also possible to calculate the number of messages that the topic/partition has so it is an another dimension

Topic State Records schema should be compatible with old and new versions of RIPT, try to avoid running different versions simultaneously on same Kafka cluster.

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
