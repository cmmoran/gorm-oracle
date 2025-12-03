package oracle

import (
	"reflect"
	"strconv"
	"strings"

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

	// Build RETURNING clause
	for i, f := range returning.fields {
		if !isReturnableField(f) {
			continue
		}
		if i > 0 {
			_ = builder.WriteByte(',')
		}
		builder.WriteQuoted(f.DBName)
	}
	if len(returning.fields) > 0 {
		_, _ = builder.WriteString(" INTO ")
	}

	rv := stmt.ReflectValue

	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}
	isSlice := rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array

	for i, f := range returning.fields {
		if !isReturnableField(f) {
			continue
		}
		if i > 0 {
			_, _ = builder.WriteString(", ")
		}

		var (
			val    reflect.Value
			valVal any
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
				valVal = ensureInitialized(val).Interface()
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
			valVal = ensureInitialized(val).Interface()
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
			v.Set(reflect.New(v.Type().Elem()))
		}
		if v.CanAddr() {
			return v.Addr()
		}
		return v
	case reflect.Slice:
		if v.IsNil() {
			v.Set(reflect.MakeSlice(v.Type(), 0, 0))
		}
		if v.CanAddr() {
			return v.Addr()
		}
		return v
	default:
		if v.CanAddr() {
			return v.Addr()
		}
		return v
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
	return f != nil && len(f.DBName) > 0 && f.Readable
}
