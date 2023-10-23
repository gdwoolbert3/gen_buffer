# Change Log

All notable changes to this project will be documented in this file.
 
## [0.2.0] - 2023-10-22

### Added

* Added the ability to provide a custom callback for determining item size (see `:size_callback` option for both
  `ExBuffer.start_link/1` and `ExBuffer.chunk!/2`)
* Added the ability to include metadata in the flush callback (see `:flush_meta` option for `ExBuffer.start_link/1`)
* Added the ability to get the time until the next scheduled flush (see `ExBuffer.next_flush/1`).

### Changed

* The `:callback` option for `ExBuffer.start_link/1` had been renamed to `:flush_callback` and now expects a function
  of arity 2 (see `ExBuffer.start_link/1`).
* Updated function docs to include doctests.
* Updated README.md to include a more sophisticated use case.
* Updated README.md to display build status and [Hex](https://hex.pm/) version.
 
## [0.1.0] - 2023-10-20
 
### Added

* Initial release
