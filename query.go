package oracle

import (
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
)

func Query(db *gorm.DB) {
	// gorm.Scan expects the names of the columns to map to the schema's fields; this usually is the case, except when it isn't
	// if the column name happens to be a reserved word (like UID or USER, etc), the column name is quoted in the Schema,
	// but the row column returned from oracle is not quoted
	if db.Statement != nil {
		if db.Statement.Schema != nil {
			fieldsByDBName := db.Statement.Schema.FieldsByDBName
			for dbName, fbdbn := range fieldsByDBName {
				if IsQuoted(dbName) {
					dbName = strings.Trim(dbName, `"`)
				}
				if _, ok := fieldsByDBName[dbName]; !ok {
					fieldsByDBName[dbName] = fbdbn
				}
			}
		}
	}
	callbacks.Query(db)
}
