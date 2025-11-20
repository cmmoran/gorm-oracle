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



## [0.3.1](https://github.com/cmmoran/gorm-oracle/compare/v0.3.0...v0.3.1) (2025-10-28)


### Bug Fixes

* fixed an issue where returning could include non-returnable fields ([073e32e](https://github.com/cmmoran/gorm-oracle/commit/073e32e297e25a6e573f468d8cb6a11b2137945e))



# [0.3.0](https://github.com/cmmoran/gorm-oracle/compare/v0.2.5...v0.3.0) (2025-10-19)


### Features

* changed dependency sijms/go-ora to cmmoran/go-ora because it is cumbersome to require go mod replace entries across many dependent projects. ([ac4881d](https://github.com/cmmoran/gorm-oracle/commit/ac4881d4ca59596ec9f64f83efc1a5fef18cca40))



## [0.2.5](https://github.com/cmmoran/gorm-oracle/compare/v0.2.4...v0.2.5) (2025-10-19)


### Bug Fixes

* removed accidental import of godoes/gorm-oracle ([9158937](https://github.com/cmmoran/gorm-oracle/commit/9158937203e4f954e8bf2429f7b48d37a4c27e31))



