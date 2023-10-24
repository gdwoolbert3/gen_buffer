# Change Log

All notable changes to this project will be documented in this file.

## [0.2.1] - 2023-10-23

### Changed

* Condensed code into a single module.

### Links

* [GitHub](https://github.com/gdwoolbert3/ex_buffer/releases/tag/v0.2.0)
 
## [0.2.0] - 2023-10-22

### Added

* Added the ability to provide a custom callback for determining item size (see `:size_callback` option for both
  `ExBuffer.start_link/1` and `ExBuffer.chunk!/2`)
* Added the ability to include metadata in the flush callback (see `:flush_meta` option for `ExBuffer.start_link/1`)
* Added the ability to get the time until the next scheduled flush (see `ExBuffer.next_flush/1`).
* Added benchee dependency for local benchmarking.

### Changed

* The `:callback` option for `ExBuffer.start_link/1` had been renamed to `:flush_callback` and now expects a function
  of arity 2 (see `ExBuffer.start_link/1`).
* Updated function docs to include doctests.
* Updated README.md to include a more sophisticated use case.
* Updated README.md to display build status and [Hex](https://hex.pm/) version.

### Links

* [GitHub](https://github.com/gdwoolbert3/ex_buffer/releases/tag/v0.2.0)
 
## [0.1.0] - 2023-10-20
 
### Added

* Initial release

### Links

* [HexDocs](https://hexdocs.pm/ex_buffer/0.1.0/readme.html)
