# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

Documentation at http://docs.blackfynn.io

## [2.1.4]
### Added
- A field for `size` to the File model

### Changed
- Models after updates to package, dataset, user, and organization API endpoints

## [2.1.3]
### Changed
- Updated `_update_self` to more reliably rely on model IDs for self verification

## [2.1.1]
### Changed
- Fixed re-authentication by maintaining organization context

## [2.1.0]
### Added
- `bf.members` function to return the members of the current organization
- Separation of CLI only and client code

### Changed
- Better working_dataset handling
- Timeseries query format has changed for the rest endpoint. Previously returned two arrays - times and values. Now returns a single array of time,value pairs. 

## [2.0.3]
### Changed
- Only create cache directory if using cache

### Added
- `bf.context.members` property to get list of current org's users

## [2.0.1]
### Changed
- Use psutil for cross-platform os process support

## [2.0.0]
### Changed
- Model after new package and datasets API endpoints

## [1.8.4]
### Added
- Environment variable support for `s3_host` and `s3_port` (used mostly internally)

### Changed
- Collection/Dataset `print_tree()` function to work with utf-8 encoded names
- Debug print on threaded upload errors.

## [1.8.1]
### Added
- Local caching
- Profile handling

### Changed
- API Tokening for login instead of username/password
- Command line interface restructuring

## [1.7.5] - 2017-07-27
### Added
- Printing path of datasets for CLI
- Appending annotation files in client (bf append, ts.append_annotation_file)
- Support upload of .bfannot alongside timeseries files
- Write annotations from timeseries package to .bfannot file (ts.write_annotations)

### Changed
- Fixes for operating on 32bit machines due to overflow from UTC timestamps

## [1.7.4] - 2017-07-14
### Added
- Bug fix for adding channel specific annotations
- Documentation additions

## [1.7.3] - 2017-07-06
### Removed
- Misc Bug Fixes

## [1.7.2] - 2017-06-29
### Added
- Optimization of uploads
- Enabled recursive upload of folders
- Enabled upload/append of annotations (.bfannot) to timeseries packages
- Ledgers

### Changed
- Reworked Annotations
	- add, insert layers and annotations functions updated
- Reduced thread count for parallel uploads

## [1.6.2] - 2017-06-01
### Added
- Initial commit

