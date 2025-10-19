package callbacks

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

func ConvertToCreateValues(stmt *gorm.Statement) (values clause.Values) {
	curTime := stmt.DB.NowFunc()

	switch value := stmt.Dest.(type) {
	case map[string]interface{}:
		values = ConvertMapToValuesForCreate(stmt, value)
	case *map[string]interface{}:
		values = ConvertMapToValuesForCreate(stmt, *value)
	case []map[string]interface{}:
		values = ConvertSliceOfMapToValuesForCreate(stmt, value)
	case *[]map[string]interface{}:
		values = ConvertSliceOfMapToValuesForCreate(stmt, *value)
	default:
		var (
			selectColumns, restricted = stmt.SelectAndOmitColumns(true, false)
			_, updateTrackTime        = stmt.Get("gorm:update_track_time")
			isZero                    bool
		)
		stmt.Settings.Delete("gorm:update_track_time")

		values = clause.Values{Columns: make([]clause.Column, 0, len(stmt.Schema.DBNames))}

		for _, db := range stmt.Schema.DBNames {
			if field := stmt.Schema.FieldsByDBName[db]; !field.HasDefaultValue || field.DefaultValueInterface != nil {
				if v, ok := selectColumns[db]; (ok && v) || (!ok && (!restricted || field.AutoCreateTime > 0 || field.AutoUpdateTime > 0)) {
					values.Columns = append(values.Columns, clause.Column{Name: db})
				}
			}
		}

		switch stmt.ReflectValue.Kind() {
		case reflect.Slice, reflect.Array:
			rValLen := stmt.ReflectValue.Len()
			if rValLen == 0 {
				_ = stmt.AddError(gorm.ErrEmptySlice)
				return
			}

			stmt.SQL.Grow(rValLen * 18)
			stmt.Vars = make([]interface{}, 0, rValLen*len(values.Columns))
			values.Values = make([][]interface{}, rValLen)

			defaultValueFieldsHavingValue := map[*schema.Field][]interface{}{}
			for i := 0; i < rValLen; i++ {
				rv := reflect.Indirect(stmt.ReflectValue.Index(i))
				if !rv.IsValid() {
					_ = stmt.AddError(fmt.Errorf("slice data #%v is invalid: %w", i, gorm.ErrInvalidData))
					return
				}

				values.Values[i] = make([]interface{}, len(values.Columns))
				for idx, column := range values.Columns {
					field := stmt.Schema.FieldsByDBName[column.Name]
					if values.Values[i][idx], isZero = field.ValueOf(stmt.Context, rv); isZero {
						if field.DefaultValueInterface != nil {
							values.Values[i][idx] = field.DefaultValueInterface
							_ = stmt.AddError(field.Set(stmt.Context, rv, field.DefaultValueInterface))
						} else if field.AutoCreateTime > 0 || field.AutoUpdateTime > 0 {
							_ = stmt.AddError(field.Set(stmt.Context, rv, curTime))
							values.Values[i][idx], _ = field.ValueOf(stmt.Context, rv)
						}
					} else if field.AutoUpdateTime > 0 && updateTrackTime {
						_ = stmt.AddError(field.Set(stmt.Context, rv, curTime))
						values.Values[i][idx], _ = field.ValueOf(stmt.Context, rv)
					}
				}

				for _, field := range stmt.Schema.FieldsWithDefaultDBValue {
					if v, ok := selectColumns[field.DBName]; (ok && v) || (!ok && !restricted) {
						if rvOfvalue, isZero := field.ValueOf(stmt.Context, rv); !isZero {
							if len(defaultValueFieldsHavingValue[field]) == 0 {
								defaultValueFieldsHavingValue[field] = make([]interface{}, rValLen)
							}
							defaultValueFieldsHavingValue[field][i] = rvOfvalue
						}
					}
				}
			}

			for _, field := range stmt.Schema.FieldsWithDefaultDBValue {
				if vs, ok := defaultValueFieldsHavingValue[field]; ok {
					values.Columns = append(values.Columns, clause.Column{Name: field.DBName})
					for idx := range values.Values {
						if vs[idx] == nil {
							values.Values[idx] = append(values.Values[idx], stmt.DefaultValueOf(field))
						} else {
							values.Values[idx] = append(values.Values[idx], vs[idx])
						}
					}
				}
			}
		case reflect.Struct:
			values.Values = [][]interface{}{make([]interface{}, len(values.Columns))}
			for idx, column := range values.Columns {
				field := stmt.Schema.FieldsByDBName[column.Name]
				if values.Values[0][idx], isZero = field.ValueOf(stmt.Context, stmt.ReflectValue); isZero {
					if field.DefaultValueInterface != nil {
						values.Values[0][idx] = field.DefaultValueInterface
						_ = stmt.AddError(field.Set(stmt.Context, stmt.ReflectValue, field.DefaultValueInterface))
					} else if field.AutoCreateTime > 0 || field.AutoUpdateTime > 0 {
						_ = stmt.AddError(field.Set(stmt.Context, stmt.ReflectValue, curTime))
						values.Values[0][idx], _ = field.ValueOf(stmt.Context, stmt.ReflectValue)
					}
				} else if field.AutoUpdateTime > 0 && updateTrackTime {
					_ = stmt.AddError(field.Set(stmt.Context, stmt.ReflectValue, curTime))
					values.Values[0][idx], _ = field.ValueOf(stmt.Context, stmt.ReflectValue)
				}
			}

			for _, field := range stmt.Schema.FieldsWithDefaultDBValue {
				if v, ok := selectColumns[field.DBName]; (ok && v) || (!ok && !restricted) && field.DefaultValueInterface == nil {
					if rvOfvalue, isZero := field.ValueOf(stmt.Context, stmt.ReflectValue); !isZero {
						values.Columns = append(values.Columns, clause.Column{Name: field.DBName})
						values.Values[0] = append(values.Values[0], rvOfvalue)
					}
				}
			}
		default:
			_ = stmt.AddError(gorm.ErrInvalidData)
		}
	}

	if c, ok := stmt.Clauses["ON CONFLICT"]; ok {
		if onConflict, _ := c.Expression.(clause.OnConflict); onConflict.UpdateAll {
			if stmt.Schema != nil && len(values.Columns) >= 1 {
				selectColumns, restricted := stmt.SelectAndOmitColumns(true, true)

				columns := make([]string, 0, len(values.Columns)-1)
				for _, column := range values.Columns {
					if field := stmt.Schema.LookUpField(column.Name); field != nil {
						if v, ok := selectColumns[field.DBName]; (ok && v) || (!ok && !restricted) {
							if !field.PrimaryKey && (!field.HasDefaultValue || field.DefaultValueInterface != nil ||
								strings.EqualFold(field.DefaultValue, "NULL")) && field.AutoCreateTime == 0 {
								if field.AutoUpdateTime > 0 {
									assignment := clause.Assignment{Column: clause.Column{Name: field.DBName}, Value: curTime}
									switch field.AutoUpdateTime {
									case schema.UnixNanosecond:
										assignment.Value = curTime.UnixNano()
									case schema.UnixMillisecond:
										assignment.Value = curTime.UnixMilli()
									case schema.UnixSecond:
										assignment.Value = curTime.Unix()
									}

									onConflict.DoUpdates = append(onConflict.DoUpdates, assignment)
								} else {
									columns = append(columns, column.Name)
								}
							}
						}
					}
				}

				onConflict.DoUpdates = append(onConflict.DoUpdates, clause.AssignmentColumns(columns)...)
				if len(onConflict.DoUpdates) == 0 {
					onConflict.DoNothing = true
				}

				// use primary fields as default OnConflict columns
				if len(onConflict.Columns) == 0 {
					for _, field := range stmt.Schema.PrimaryFields {
						onConflict.Columns = append(onConflict.Columns, clause.Column{Name: field.DBName})
					}
				}
				stmt.AddClause(onConflict)
			}
		}
	}

	return values
}

// ConvertMapToValuesForCreate convert map to values
func ConvertMapToValuesForCreate(stmt *gorm.Statement, mapValue map[string]interface{}) (values clause.Values) {
	values.Columns = make([]clause.Column, 0, len(mapValue))
	selectColumns, restricted := stmt.SelectAndOmitColumns(true, false)

	keys := make([]string, 0, len(mapValue))
	for k := range mapValue {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		value := mapValue[k]
		if stmt.Schema != nil {
			if field := stmt.Schema.LookUpField(k); field != nil {
				k = field.DBName
			}
		}

		if v, ok := selectColumns[k]; (ok && v) || (!ok && !restricted) {
			values.Columns = append(values.Columns, clause.Column{Name: k})
			if len(values.Values) == 0 {
				values.Values = [][]interface{}{{}}
			}

			values.Values[0] = append(values.Values[0], value)
		}
	}
	return
}

// ConvertSliceOfMapToValuesForCreate convert slice of map to values
func ConvertSliceOfMapToValuesForCreate(stmt *gorm.Statement, mapValues []map[string]interface{}) (values clause.Values) {
	columns := make([]string, 0, len(mapValues))

	// when the length of mapValues is zero,return directly here
	// no need to call stmt.SelectAndOmitColumns method
	if len(mapValues) == 0 {
		_ = stmt.AddError(gorm.ErrEmptySlice)
		return
	}

	var (
		result                    = make(map[string][]interface{}, len(mapValues))
		selectColumns, restricted = stmt.SelectAndOmitColumns(true, false)
	)

	for idx, mapValue := range mapValues {
		for k, v := range mapValue {
			if stmt.Schema != nil {
				if field := stmt.Schema.LookUpField(k); field != nil {
					k = field.DBName
				}
			}

			if _, ok := result[k]; !ok {
				if v, ok := selectColumns[k]; (ok && v) || (!ok && !restricted) {
					result[k] = make([]interface{}, len(mapValues))
					columns = append(columns, k)
				} else {
					continue
				}
			}

			result[k][idx] = v
		}
	}

	sort.Strings(columns)
	values.Values = make([][]interface{}, len(mapValues))
	values.Columns = make([]clause.Column, len(columns))
	for idx, column := range columns {
		values.Columns[idx] = clause.Column{Name: column}

		for i, v := range result[column] {
			if len(values.Values[i]) == 0 {
				values.Values[i] = make([]interface{}, len(columns))
			}

			values.Values[i][idx] = v
		}
	}
	return
}

type visitMap = map[reflect.Value]bool

// Check if circular values, return true if loaded
func loadOrStoreVisitMap(visitMap *visitMap, v reflect.Value) (loaded bool) {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		loaded = true
		for i := 0; i < v.Len(); i++ {
			if !loadOrStoreVisitMap(visitMap, v.Index(i)) {
				loaded = false
			}
		}
	case reflect.Struct, reflect.Interface:
		if v.CanAddr() {
			p := v.Addr()
			if _, ok := (*visitMap)[p]; ok {
				return true
			}
			(*visitMap)[p] = true
		}
	}

	return
}
