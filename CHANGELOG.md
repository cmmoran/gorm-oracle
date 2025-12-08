## [0.5.1](https://github.com/cmmoran/gorm-oracle/compare/v0.5.0...v0.5.1) (2025-12-08)


### Bug Fixes

* **chunk:** enhance IN clause handling with chunking and support for negation ([ac59f52](https://github.com/cmmoran/gorm-oracle/commit/ac59f529cf3027d65dc8caa84cdf0ed001609ff8))



# [0.5.0](https://github.com/cmmoran/gorm-oracle/compare/v0.4.2...v0.5.0) (2025-12-03)


### Features

* add support for nullable reference pointer fields and fix RETURNING initialization logic ([8f476f2](https://github.com/cmmoran/gorm-oracle/commit/8f476f2f001c444f855fb67f0eebc7eed16b36e3))



## [0.4.2](https://github.com/cmmoran/gorm-oracle/compare/v0.4.1...v0.4.2) (2025-12-01)


### Bug Fixes

* adjust column mapping to support custom naming strategies ([5005107](https://github.com/cmmoran/gorm-oracle/commit/50051079d459b3b76f013d9debdc419caa25918a))



## [0.4.1](https://github.com/cmmoran/gorm-oracle/compare/v0.4.0...v0.4.1) (2025-11-20)


### Bug Fixes

* replaced `instantiateNilPointers` with `ensureInitialized` for cleaner initialization logic for RETURNING clause. ([94166c2](https://github.com/cmmoran/gorm-oracle/commit/94166c2f4dfa7fa7a95a00cfa5899f14399282e9))



# [0.4.0](https://github.com/cmmoran/gorm-oracle/compare/v0.3.1...v0.4.0) (2025-11-19)


### Bug Fixes

* add schema check in where clause handling for oracle dialect ([9d97650](https://github.com/cmmoran/gorm-oracle/commit/9d976502b3e372805295f47d2be9828b3d74e434))
* fixing go-ora parameter handling ([de0293f](https://github.com/cmmoran/gorm-oracle/commit/de0293f99621c5bff282c884eee04749346f992e))
* remove debug print statements from where clause handling ([14aeb49](https://github.com/cmmoran/gorm-oracle/commit/14aeb49024c4a684e295a8bfe81c993c8a3e93d6))
* replace hardcoded NLS formats with constants ([937c224](https://github.com/cmmoran/gorm-oracle/commit/937c224ae8239e4eaae054c7808f56e76509e754))


### Features

* add IN clause chunking for oracle where expressions ([7d39494](https://github.com/cmmoran/gorm-oracle/commit/7d394949a3c88610052732fe31daf6115d8eccec))
* add query support for oracle dialect and enhance timestamp handling in gorm ([5a22be4](https://github.com/cmmoran/gorm-oracle/commit/5a22be49916606119cd3864a7f590a00c0e190ed))



