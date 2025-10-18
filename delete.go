package oracle

import (
	"reflect"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

func Delete(db *gorm.DB) {
	if db.Error != nil {
		return
	}

	if db.Statement.Schema != nil {
		for _, c := range db.Statement.Schema.DeleteClauses {
			db.Statement.AddClause(c)
		}
	}

	if db.Statement.SQL.Len() == 0 {
		db.Statement.SQL.Grow(100)
		db.Statement.AddClauseIfNotExists(clause.Delete{})

		if db.Statement.Schema != nil {
			_, queryValues := schema.GetIdentityFieldValuesMap(db.Statement.Context, db.Statement.ReflectValue, db.Statement.Schema.PrimaryFields)
			column, values := schema.ToQueryValues(db.Statement.Table, db.Statement.Schema.PrimaryFieldDBNames, queryValues)

			if len(values) > 0 {
				db.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
			}

			if db.Statement.ReflectValue.CanAddr() && db.Statement.Dest != db.Statement.Model && db.Statement.Model != nil {
				_, queryValues = schema.GetIdentityFieldValuesMap(db.Statement.Context, reflect.ValueOf(db.Statement.Model), db.Statement.Schema.PrimaryFields)
				column, values = schema.ToQueryValues(db.Statement.Table, db.Statement.Schema.PrimaryFieldDBNames, queryValues)

				if len(values) > 0 {
					db.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
				}
			}
		}

		db.Statement.AddClauseIfNotExists(clause.From{})

		if returning := ReturningWithPrimaryFields(db.Statement.Schema); len(returning.Names) > 0 {
			db.Statement.AddClause(returning)
		} else {
			db.Statement.AddClauseIfNotExists(clause.Returning{})
		}

		db.Statement.Build(db.Statement.BuildClauses...)
	}

	checkMissingWhereConditions(db)

	if !db.DryRun && db.Error == nil {
		result, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)

		if db.AddError(err) == nil {
			db.RowsAffected, _ = result.RowsAffected()

			if db.Statement.Result != nil {
				db.Statement.Result.Result = result
				db.Statement.Result.RowsAffected = db.RowsAffected
			}
		}
	}
}
