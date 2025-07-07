package oracle

import (
	"fmt"
	go_ora "github.com/sijms/go-ora/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"gorm.io/gorm/utils"
	"reflect"
	"sort"
	"time"
)

func Update(config *callbacks.Config) func(db *gorm.DB) {
	_ = utils.Contains(config.UpdateClauses, "RETURNING")

	return func(db *gorm.DB) {
		if db.Error != nil {
			return
		}

		stmt := db.Statement
		if stmt == nil {
			return
		}

		if stmtSchema := stmt.Schema; stmtSchema != nil {
			for _, c := range stmtSchema.UpdateClauses {
				stmt.AddClause(c)
			}
		}

		if stmt.SQL.Len() == 0 {
			stmt.SQL.Grow(180)
			stmt.AddClauseIfNotExists(clause.Update{})
			if _, ok := stmt.Clauses["SET"]; !ok {
				if set := ConvertToAssignments(stmt); len(set) != 0 {
					defer delete(stmt.Clauses, "SET")
					stmt.AddClause(set)
				} else {
					return
				}
			}

			stmt.Build(stmt.BuildClauses...)
			bindOracleReturning(stmt)
		}

		checkMissingWhereConditions(db)

		if !db.DryRun && db.Error == nil {
			result, err := stmt.ConnPool.ExecContext(stmt.Context, stmt.SQL.String(), stmt.Vars...)

			if db.AddError(err) == nil {
				db.RowsAffected, _ = result.RowsAffected()
			}
		}
	}
}

var skipTypes = map[reflect.Type]struct{}{
	reflect.TypeOf((*time.Time)(nil)): {},
}

func instantiateNilPointers(v reflect.Value) {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			// Don't preallocate root pointer here — expect caller to pass a pointer-to-struct
			return
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return
	}

	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := typ.Field(i)

		// Skip unexported fields
		if fieldType.PkgPath != "" {
			continue
		}

		switch field.Kind() {
		case reflect.Ptr:
			// Skip certain types
			if _, skip := skipTypes[field.Type()]; skip {
				continue
			}
			if field.IsNil() {
				// Instantiate pointer
				field.Set(reflect.New(field.Type().Elem()))
			}
			// Recurse
			instantiateNilPointers(field)

		case reflect.Struct:
			instantiateNilPointers(field)
		default:
		}
	}
}

func bindOracleReturning(stmt *gorm.Statement) {
	// Locate the RETURNING clause
	ret, ok := stmt.Clauses[clause.Returning{}.Name()]
	if !ok {
		return
	}
	cols := ret.Expression.(clause.Returning).Columns
	_, _ = stmt.WriteString(" INTO ")

	// Determine the struct root for binding
	var rv reflect.Value
	if !stmt.ReflectValue.IsValid() && stmt.Model != nil {
		rv = reflect.ValueOf(stmt.Model)
		for rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
	} else {
		rv = stmt.ReflectValue
		instantiateNilPointers(rv)
		for rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
	}
	if len(cols) == 0 {
		for _, f := range stmt.Schema.Fields {
			cols = append(cols, clause.Column{Name: f.DBName})
		}
	}

	// Append an OUT bind for each column
	for i, col := range cols {
		// Match column to schema field
		var sf *schema.Field
		for _, f := range stmt.Schema.Fields {
			name := stmt.DB.NamingStrategy.ColumnName("", col.Name)
			if f.DBName == name {
				sf = f
				break
			}
		}
		// Determine destination pointer
		var (
			destPtr interface{}
			size    int
		)
		if sf != nil {
			field := rv.FieldByName(sf.Name)

			var refVal reflect.Value
			refVal = sf.ReflectValueOf(stmt.Context, rv)
			if refVal.IsValid() && refVal.CanAddr() {
				destPtr = refVal.Addr().Interface()
			} else {
				destPtr = field.Interface()
			}
			if field.Kind() == reflect.Ptr {
				field = field.Elem()
			}

			switch field.Kind() {
			case reflect.Bool:
				if sf.Size == 0 {
					size = 64
				} else {
					size = sf.Size
				}
			case reflect.String:
				if sf.Size == 0 {
					size = 4000
				} else {
					size = sf.Size
				}
			default:
				size = sf.Size
			}
		} else {
			var tmp interface{}
			destPtr = &tmp
		}
		if i > 0 {
			_, _ = stmt.WriteString(", ")
		}
		stmt.AddVar(stmt, go_ora.Out{
			Dest: destPtr,
			Size: size,
			In:   true,
		})
		_, _ = stmt.WriteString(fmt.Sprintf("/*- %s -*/", col.Name))
	}
}

func checkMissingWhereConditions(db *gorm.DB) {
	if !db.AllowGlobalUpdate && db.Error == nil {
		where, withCondition := db.Statement.Clauses["WHERE"]
		if withCondition {
			if _, withSoftDelete := db.Statement.Clauses["soft_delete_enabled"]; withSoftDelete {
				whereClause, _ := where.Expression.(clause.Where)
				withCondition = len(whereClause.Exprs) > 1
			}
		}
		if !withCondition {
			_ = db.AddError(gorm.ErrMissingWhereClause)
		}
		return
	}
}

// ConvertToAssignments convert to update assignments
func ConvertToAssignments(stmt *gorm.Statement) (set clause.Set) {
	var (
		selectColumns, restricted = stmt.SelectAndOmitColumns(false, true)
		assignValue               func(field *schema.Field, value interface{})
	)

	switch stmt.ReflectValue.Kind() {
	case reflect.Slice, reflect.Array:
		assignValue = func(field *schema.Field, value interface{}) {
			for i := 0; i < stmt.ReflectValue.Len(); i++ {
				if stmt.ReflectValue.CanAddr() {
					_ = field.Set(stmt.Context, stmt.ReflectValue.Index(i), value)
				}
			}
		}
	case reflect.Struct:
		assignValue = func(field *schema.Field, value interface{}) {
			if stmt.ReflectValue.CanAddr() {
				_ = field.Set(stmt.Context, stmt.ReflectValue, value)
			}
		}
	default:
		assignValue = func(field *schema.Field, value interface{}) {
		}
	}

	updatingValue := reflect.ValueOf(stmt.Dest)
	for updatingValue.Kind() == reflect.Ptr {
		updatingValue = updatingValue.Elem()
	}

	if !updatingValue.CanAddr() || stmt.Dest != stmt.Model {
		switch stmt.ReflectValue.Kind() {
		case reflect.Slice, reflect.Array:
			if size := stmt.ReflectValue.Len(); size > 0 {
				var isZero bool
				for i := 0; i < size; i++ {
					for _, field := range stmt.Schema.PrimaryFields {
						_, isZero = field.ValueOf(stmt.Context, stmt.ReflectValue.Index(i))
						if !isZero {
							break
						}
					}
				}

				if !isZero {
					_, primaryValues := schema.GetIdentityFieldValuesMap(stmt.Context, stmt.ReflectValue, stmt.Schema.PrimaryFields)
					column, values := schema.ToQueryValues("", stmt.Schema.PrimaryFieldDBNames, primaryValues)
					stmt.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
				}
			}
		case reflect.Struct:
			for _, field := range stmt.Schema.PrimaryFields {
				if value, isZero := field.ValueOf(stmt.Context, stmt.ReflectValue); !isZero {
					stmt.AddClause(clause.Where{Exprs: []clause.Expression{clause.Eq{Column: field.DBName, Value: value}}})
				}
			}
		default:
		}
	}

	switch value := updatingValue.Interface().(type) {
	case map[string]interface{}:
		set = make([]clause.Assignment, 0, len(value))

		keys := make([]string, 0, len(value))
		for k := range value {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			kv := value[k]
			if _, ok := kv.(*gorm.DB); ok {
				kv = []interface{}{kv}
			}

			if stmt.Schema != nil {
				if field := stmt.Schema.LookUpField(k); field != nil {
					if field.DBName != "" {
						if v, ok := selectColumns[field.DBName]; (ok && v) || (!ok && !restricted) {
							set = append(set, clause.Assignment{Column: clause.Column{Name: field.DBName}, Value: kv})
							assignValue(field, value[k])
						}
					} else if v, ok := selectColumns[field.Name]; (ok && v) || (!ok && !restricted) {
						assignValue(field, value[k])
					}
					continue
				}
			}

			if v, ok := selectColumns[k]; (ok && v) || (!ok && !restricted) {
				set = append(set, clause.Assignment{Column: clause.Column{Name: k}, Value: kv})
			}
		}

		if !stmt.SkipHooks && stmt.Schema != nil {
			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.LookUpField(dbName)
				if field.AutoUpdateTime > 0 && value[field.Name] == nil && value[field.DBName] == nil {
					if v, ok := selectColumns[field.DBName]; (ok && v) || !ok {
						now := stmt.DB.NowFunc()
						assignValue(field, now)

						if field.AutoUpdateTime == schema.UnixNanosecond {
							set = append(set, clause.Assignment{Column: clause.Column{Name: field.DBName}, Value: now.UnixNano()})
						} else if field.AutoUpdateTime == schema.UnixMillisecond {
							set = append(set, clause.Assignment{Column: clause.Column{Name: field.DBName}, Value: now.UnixNano() / 1e6})
						} else if field.AutoUpdateTime == schema.UnixSecond {
							set = append(set, clause.Assignment{Column: clause.Column{Name: field.DBName}, Value: now.Unix()})
						} else {
							set = append(set, clause.Assignment{Column: clause.Column{Name: field.DBName}, Value: now})
						}
					}
				}
			}
		}
	default:
		updatingSchema := stmt.Schema
		var isDiffSchema bool
		if !updatingValue.CanAddr() || stmt.Dest != stmt.Model {
			// different schema
			updatingStmt := &gorm.Statement{DB: stmt.DB}
			if err := updatingStmt.Parse(stmt.Dest); err == nil {
				updatingSchema = updatingStmt.Schema
				isDiffSchema = true
			}
		}

		switch updatingValue.Kind() {
		case reflect.Struct:
			set = make([]clause.Assignment, 0, len(stmt.Schema.FieldsByDBName))
			for _, dbName := range stmt.Schema.DBNames {
				if field := updatingSchema.LookUpField(dbName); field != nil {
					if !field.PrimaryKey || !updatingValue.CanAddr() || stmt.Dest != stmt.Model {
						if v, ok := selectColumns[field.DBName]; (ok && v) || (!ok && (!restricted || (!stmt.SkipHooks && field.AutoUpdateTime > 0))) {
							value, isZero := field.ValueOf(stmt.Context, updatingValue)
							if !stmt.SkipHooks && field.AutoUpdateTime > 0 {
								if field.AutoUpdateTime == schema.UnixNanosecond {
									value = stmt.DB.NowFunc().UnixNano()
								} else if field.AutoUpdateTime == schema.UnixMillisecond {
									value = stmt.DB.NowFunc().UnixNano() / 1e6
								} else if field.AutoUpdateTime == schema.UnixSecond {
									value = stmt.DB.NowFunc().Unix()
								} else {
									value = stmt.DB.NowFunc()
								}
								isZero = false
							}

							if (ok || !isZero) && field.Updatable {
								value = convertCustomType(value)
								set = append(set, clause.Assignment{Column: clause.Column{Name: field.DBName}, Value: value})
								assignField := field
								if isDiffSchema {
									if originField := stmt.Schema.LookUpField(dbName); originField != nil {
										assignField = originField
									}
								}
								assignValue(assignField, value)
							}
						}
					} else {
						if value, isZero := field.ValueOf(stmt.Context, updatingValue); !isZero {
							stmt.AddClause(clause.Where{Exprs: []clause.Expression{clause.Eq{Column: field.DBName, Value: value}}})
						}
					}
				}
			}
		default:
			_ = stmt.AddError(gorm.ErrInvalidData)
		}
	}

	return
}
