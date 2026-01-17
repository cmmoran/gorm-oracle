package oracle

import (
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cmmoran/go-ora/v2"
	regexp "github.com/dlclark/regexp2"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

var stringTypeWithSize = regexp.MustCompile(`(?i)\b(?:varchar2?|nvarchar2|nchar|char)\s*\(\s*(\d+)(?:\s+(?:byte|char))?\s*\)`, regexp.RE2)

func ReturningFieldsWithDefaultDBValue(sch *schema.Schema, values *clause.Values) Returning {
	if sch == nil {
		return Returning{}
	}
	r := Returning{
		Names:  make([]string, 0),
		fields: sch.FieldsWithDefaultDBValue,
		vars:   values,
	}
	for _, field := range r.fields {
		r.Names = append(r.Names, field.DBName)
	}

	return r
}

func ReturningWithPrimaryFields(sch *schema.Schema) Returning {
	if sch == nil {
		return Returning{}
	}
	r := Returning{
		Names:  make([]string, 0),
		fields: sch.PrimaryFields,
	}
	for _, field := range r.fields {
		r.Names = append(r.Names, field.DBName)
	}

	return r
}

func ReturningWithColumns(cols []clause.Column) Returning {
	r := Returning{
		Names: make([]string, 0),
	}
	for _, col := range cols {
		r.Names = append(r.Names, col.Name)
	}

	return r
}

type Returning struct {
	Names  []string
	fields []*schema.Field
	vars   *clause.Values
}

// Name where clause name
func (returning Returning) Name() string {
	return "RETURNING"
}

func (returning Returning) Build(builder clause.Builder) {
	stmt, ok := builder.(*gorm.Statement)
	if !ok || stmt.Schema == nil {
		return
	}

	// Collect fields
	if len(returning.fields) == 0 {
		if len(returning.Names) > 0 {
			for _, n := range returning.Names {
				if f := stmt.Schema.LookUpField(n); f != nil && isReturnableField(f) {
					returning.fields = append(returning.fields, f)
				}
			}
		} else {
			for _, f := range stmt.Schema.Fields {
				if isReturnableField(f) {
					returning.Names = append(returning.Names, f.DBName)
					returning.fields = append(returning.fields, f)
				}
			}
		}
	} else if len(returning.Names) == 0 {
		for _, f := range returning.fields {
			if isReturnableField(f) {
				returning.Names = append(returning.Names, f.DBName)
			}
		}
	}

	if len(returning.fields) == 0 {
		return
	}

	rv := stmt.ReflectValue

	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}
	isSlice := rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array

	filteredFields := make([]*schema.Field, 0, len(returning.fields))
	for _, f := range returning.fields {
		if !isReturnableField(f) {
			continue
		}
		if !canBindReturningField(stmt, rv, f) {
			continue
		}
		filteredFields = append(filteredFields, f)
	}

	if len(filteredFields) == 0 {
		return
	}

	// Build RETURNING clause
	for i, f := range filteredFields {
		if i > 0 {
			_ = builder.WriteByte(',')
		}
		builder.WriteQuoted(f.DBName)
	}
	_, _ = builder.WriteString(" INTO ")

	for i, f := range filteredFields {
		if i > 0 {
			_, _ = builder.WriteString(", ")
		}

		var (
			val    reflect.Value
			valVal any
			ok     bool
			size   = max(1, f.Size)
		)
		if f.Size == 0 {
			dt := f.DataType
			if match, err := stringTypeWithSize.FindStringMatch(strings.ToLower(string(dt))); err == nil && match != nil {
				if match.GroupByNumber(1) != nil {
					size, err = strconv.Atoi(match.GroupByNumber(1).String())
					if err != nil {
						size = 128
					}
				}
			}
		}
		if isSlice {
			rows := rv.Len()

			for j := 0; j < rows; j++ {
				elem := rv.Index(j)
				for elem.Kind() == reflect.Ptr {
					elem = elem.Elem()
				}
				val = f.ReflectValueOf(stmt.Context, elem)
				valVal, ok = returningDest(val)
				if !ok {
					return
				}
				out := go_ora.Out{
					Dest: valVal,
					Size: size,
				}
				if returning.vars != nil && len(returning.vars.Values) > j {
					returning.vars.Values[j] = append(returning.vars.Values[j], out)
				}
				if j == 0 {
					builder.AddVar(stmt, out)
				}
			}
		} else {
			val = f.ReflectValueOf(stmt.Context, rv)
			valVal, ok = returningDest(val)
			if !ok {
				return
			}
			out := go_ora.Out{
				Dest: valVal,
				Size: size,
			}
			if returning.vars != nil && len(returning.vars.Values) > 0 {
				returning.vars.Values[0] = append(returning.vars.Values[0], out)
			}
			builder.AddVar(stmt, out)
		}
	}
}

func ensureInitialized(v reflect.Value) reflect.Value {
	if !v.IsValid() {
		return v
	}
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			if v.CanSet() {
				v.Set(reflect.New(v.Type().Elem()))
			} else {
				return reflect.New(v.Type().Elem())
			}
		}
		if v.CanAddr() {
			return v.Addr()
		}
		return v
	case reflect.Slice:
		if v.IsNil() {
			if v.CanSet() {
				v.Set(reflect.MakeSlice(v.Type(), 0, 0))
			} else {
				p := reflect.New(v.Type())
				p.Elem().Set(reflect.MakeSlice(v.Type(), 0, 0))
				return p
			}
		}
		if v.CanAddr() {
			return v.Addr()
		}
		p := reflect.New(v.Type())
		p.Elem().Set(v)
		return p
	default:
		if v.CanAddr() {
			return v.Addr()
		}
		p := reflect.New(v.Type())
		p.Elem().Set(v)
		return p
	}
}

var (
	scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	timeType    = reflect.TypeOf(time.Time{})
)

func isScalarOutType(t reflect.Type) bool {
	if t == nil {
		return false
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
		if t == nil {
			return false
		}
	}
	if t == timeType {
		return true
	}
	if t.Implements(scannerType) || reflect.PointerTo(t).Implements(scannerType) {
		return true
	}
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.String, reflect.Interface:
		return true
	case reflect.Slice:
		return t.Elem().Kind() == reflect.Uint8
	default:
		return false
	}
}

func canBindReturningField(stmt *gorm.Statement, rv reflect.Value, f *schema.Field) bool {
	if !rv.IsValid() {
		return false
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		if rv.Len() == 0 {
			return false
		}
		elem := rv.Index(0)
		for elem.Kind() == reflect.Ptr {
			if elem.IsNil() {
				return false
			}
			elem = elem.Elem()
		}
		val := f.ReflectValueOf(stmt.Context, elem)
		return canUseReturningDest(val)
	}
	val := f.ReflectValueOf(stmt.Context, rv)
	return canUseReturningDest(val)
}

func canUseReturningDest(v reflect.Value) bool {
	if !v.IsValid() {
		return false
	}
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return v.CanSet()
		}
		return true
	case reflect.Slice:
		if v.IsNil() {
			return v.CanSet()
		}
		return v.CanAddr()
	case reflect.Interface:
		return v.CanAddr()
	default:
		return v.CanAddr()
	}
}

func returningDest(v reflect.Value) (any, bool) {
	if !v.IsValid() {
		return nil, false
	}
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			if v.CanSet() {
				v.Set(reflect.New(v.Type().Elem()))
			} else {
				return nil, false
			}
		}
		return v.Interface(), true
	case reflect.Slice:
		if v.IsNil() {
			if v.CanSet() {
				v.Set(reflect.MakeSlice(v.Type(), 0, 0))
			} else {
				return nil, false
			}
		}
		if v.CanAddr() {
			return v.Addr().Interface(), true
		}
		return nil, false
	default:
		if v.CanAddr() {
			return v.Addr().Interface(), true
		}
		return nil, false
	}
}

// MergeClause merge order by clauses
func (returning Returning) MergeClause(clause *clause.Clause) {
	if v, ok := clause.Expression.(Returning); ok && len(returning.fields) > 0 {
		if v.Names != nil {
			returning.Names = append(v.Names, returning.Names...)
		} else {
			returning.Names = nil
		}
		if v.fields != nil {
			returning.fields = append(v.fields, returning.fields...)
		} else {
			returning.fields = nil
		}
	}
	clause.Expression = returning
}

func isReturnableField(f *schema.Field) bool {
	if f == nil || len(f.DBName) == 0 || !f.Readable {
		return false
	}
	if f.EmbeddedSchema != nil {
		return false
	}
	return isScalarOutType(f.FieldType)
}
