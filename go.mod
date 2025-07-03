module github.com/cmmoran/gorm-oracle

go 1.24.4

require (
	github.com/emirpasic/gods v1.18.1
	github.com/google/uuid v1.6.0
	github.com/sijms/go-ora/v2 v2.9.0
	gorm.io/gorm v1.30.0
)

require (
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	golang.org/x/text v0.26.0 // indirect
)

exclude (
	github.com/sijms/go-ora/v2 v2.8.8 // ORA-03137: [opiexe: protocol violation]
	github.com/sijms/go-ora/v2 v2.8.9 // buggy
)
