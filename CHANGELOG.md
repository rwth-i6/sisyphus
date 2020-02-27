# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Support for async workflow definitions. If await tk.async_run(obj) is called sisyphus will wait until all Path objects inside of obj are available
- Path has now is_set method
- Manager can now be paused

### Fixed
- Avoid crash if tracemalloc not found, needed to run sisyphus in pypy
- Added timeout to html visualization which can fail for large graphs

## [1.0.0] - 2020-02-11

First release using a versioning scheme.

### Features

- Workflow definition is fully compatible python3 code
- Compute Jobs that produce the same output only once to save time and space
- Support for multiple cluster engines, e.g. Sun Grid Engine (SGE) including its closely related forks, Platform Load Sharing Facility (LSF), and build in local engine to start jobs on the same computer as Sisyphus
- Restart of failed jobs, if necessary with increased requirements
- Builtin tools to clean up old jobs
- IPython console to modify graph directly
- Web server to display running jobs and dependency graph
- Console user interface using urwid
- and more

[Unreleased]: https://github.com/rwth-i6/sisyphus/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/rwth-i6/sisyphus/releases/tag/v1.0.0
