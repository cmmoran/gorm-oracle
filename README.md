# GORM Oracle Driver

## Description

GORM Oracle driver for connect Oracle DB and Manage Oracle DB, Based on [godoes/oracle](https://github.com/godoes/gorm-oracle) with changes to support partial indexes and any `[16]byte` type-alias ( ex: uuid / ulid / etc ) to / from `raw(16)` conversion.

## Required dependency Install

#### Note: oracle client is _not_ needed

- Oracle `12c` (or higher)
- Golang `v1.24+`
- gorm `1.24.0` + (tested with `1.31.0`)

## Quick Start

### How to install 

```bash
go get -d github.com/cmmoran/gorm-oracle
```

### Usage

```go
package main

import (
	oracle "github.com/cmmoran/gorm-oracle"
	"gorm.io/gorm"
)

func main() {
	options := map[string]string{
		"CONNECTION TIMEOUT": "90",
		"SSL":                "false",
	}
	// oracle://user:password@127.0.0.1:1521/service_name
	url := oracle.BuildUrl("127.0.0.1", 1521, "service", "user", "password", options)
	dialector := oracle.New(oracle.Config{
		DSN:                     url,
		IgnoreCase:              false, // query conditions are not case-sensitive
		PreferedCase:           oracle.ScreamingSnakeCase, // this matches oracles default handling of identifiers; other options are SnakeCase, CamelCase which will force NamingCaseSensitive to true
		NamingCaseSensitive:     true,  // whether naming is case-sensitive
		VarcharSizeIsCharLength: true,  // whether VARCHAR type size is character length, defaulting to byte length

		// RowNumberAliasForOracle11 is the alias for ROW_NUMBER() in Oracle 11g, defaulting to ROW_NUM
		RowNumberAliasForOracle11: "ROW_NUM",
	})
	cfg := &gorm.Config{
      SkipDefaultTransaction:                   true,
      DisableForeignKeyConstraintWhenMigrating: true,
      NamingStrategy: schema.NamingStrategy{
        IdentifierMaxLength: 30,   // Oracle >= 12.2: 128, Oracle < 12.2: 30, PostgreSQL:63, MySQL: 64, SQL Server、SQLite、DM: 128
      },
      PrepareStmt:     false,
      CreateBatchSize: 50,
    }
	db, err := gorm.Open(dialector, cfg)
	if err != nil {
		// panic error or log error info
	}

	// set session parameters
	if sqlDB, err := db.DB(); err == nil {
		_, _ = oracle.AddSessionParams(sqlDB, map[string]string{
			"TIME_ZONE":                "+0:00",                                 // ALTER SESSION SET TIME_ZONE = '+0:00';
			// try to match go defaults as closely as possible
			"NLS_DATE_FORMAT":          `YYYY-MM-DD"T"HH24:MI:SS`,               // ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS'; 
			"NLS_TIMESTAMP_FORMAT":     `YYYY-MM-DD"T"HH24:MI:SS.FF6`,           // ALTER SESSION SET NLS_TIME_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.FF6';
			"NLS_TIMESTAMP_TZ_FORMAT":  `YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM`,    // ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM';
			"NLS_TIME_FORMAT":          `HH24:MI:SS.FF6`,                        // ALTER SESSION SET NLS_TIME_TZ_FORMAT = 'HH24:MI:SS.FF6';
			"NLS_TIME_TZ_FORMAT":       `HH24:MI:SS.FF6TZH:TZM`,                 // ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'HH24:MI:SS.FF6TZH:TZM';
			//"NLS_DATE_FORMAT":         "YYYY-MM-DD",                   
			//"NLS_TIME_FORMAT":         "HH24:MI:SSXFF",                
			//"NLS_TIMESTAMP_FORMAT":    "YYYY-MM-DD HH24:MI:SSXFF",     
			//"NLS_TIME_TZ_FORMAT":      "HH24:MI:SS.FF TZR",            
			//"NLS_TIMESTAMP_TZ_FORMAT": "YYYY-MM-DD HH24:MI:SSXFF TZR", 
		})
	}

	// do stuff
}

```

## Questions

<!--suppress HtmlDeprecatedAttribute -->
<details>
<summary>ORA-01000: maximum open cursors exceeded</summary>

> ORA-00604: error occurred at recursive SQL level 1
> 
> ORA-01000: maximum open cursors exceeded

```shell
show parameter OPEN_CURSORS;
```

```sql
alter system set OPEN_CURSORS = 1000; -- or bigger
commit;
```

</details>

<details>
<summary>ORA-01002: fetch out of sequence</summary>

> If the same query is executed repeatedly, and the first query is successful but the second one returns an `ORA-01002` error, it might be because `PrepareStmt` is enabled.  Disabling this configuration should resolve the issue.

Recommended configuration:

```go
&gorm.Config{
    SkipDefaultTransaction:                   true, // Should single create, update, and delete operations be disabled from automatically executing within transactions?
    DisableForeignKeyConstraintWhenMigrating: true, // Is it possible to disable the automatic creation of foreign key constraints when automatically migrating or creating tables?
    // Custom naming strategy
	NamingStrategy: oracle.NamingStrategy{
            PreferredCase:       oracle.ScreamingSnakeCase, 
            IdentifierMaxLength: 128,   
    },
    PrepareStmt:     false, // Create and cache precompiled statements.  Enabling this may result in an ORA-01002 error.
    CreateBatchSize: 50,    // Default batch size for inserting data
}
```

</details>

<details>
<summary>UUID / ULID / [16]byte type-alias support</summary>

> How do I get `UUID` support?

Unfortunately, sijms/go-ora does not support `UUID` as a _first-class citizen`. However, in your go.mod file add this:

```go
replace github.com/sijms/go-ora/v2 => github.com/cmmoran/go-ora/v2 v2.0.0-20250926150009-a7656b5212f0

```

I've added first-class support for _uuid-as-string_ and any other `[16]byte` type-alias within go-ora. At this time

I have not yet opened a PR to merge these changes into the original repo.

I have added this change to the `gorm-oracle` go.mod file but this will only work for you if you also add it.
</details>
