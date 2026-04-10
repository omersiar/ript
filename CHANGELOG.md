# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- RIPT_STATIC_FILES_DIR directory configuration for http server static files

### Changed

- the state snapshots are now produced more efficiently 

### Fixed

- RIPT would not terminate gracefully when broker connection is lost
- the default topic creation timeout
- a condition where test harness exceeds [MAX_PARTITIONS_PER_BATCH](https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L152C22-L152C46) limit

## v0.0.1 - 2026-04-04

### Added

- Initial version
