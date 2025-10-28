package oracle

import (
	"reflect"
	"time"

	"github.com/cmmoran/go-ora/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

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
	instantiateNilPointers(rv)

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
			val, fbn reflect.Value
			valVal   any
		)
		if isSlice {
			rows := rv.Len()

			for j := 0; j < rows; j++ {
				elem := rv.Index(j)
				for elem.Kind() == reflect.Ptr {
					elem = elem.Elem()
				}
				fbn = elem.FieldByName(f.Name)
				val = f.ReflectValueOf(stmt.Context, elem)
				if val.IsValid() && val.CanAddr() {
					valVal = val.Addr().Interface()
				} else {
					valVal = fbn.Interface()
				}
				out := go_ora.Out{
					Dest: valVal,
					Size: max(1, f.Size),
				}
				if returning.vars != nil && len(returning.vars.Values) > j {
					returning.vars.Values[j] = append(returning.vars.Values[j], out)
				}
				if j == 0 {
					builder.AddVar(stmt, out)
				}
			}
		} else {
			fbn = rv.FieldByName(f.Name)
			val = f.ReflectValueOf(stmt.Context, rv)
			if val.IsValid() && val.CanAddr() {
				valVal = val.Addr().Interface()
			} else {
				valVal = fbn.Interface()
			}
			out := go_ora.Out{
				Dest: valVal,
				Size: max(1, f.Size),
			}
			if returning.vars != nil && len(returning.vars.Values) > 0 {
				returning.vars.Values[0] = append(returning.vars.Values[0], out)
			}
			builder.AddVar(stmt, out)
		}
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

var skipTypes = map[reflect.Type]struct{}{
	reflect.TypeOf((*time.Time)(nil)): {},
}

func instantiateNilPointers(root interface{}) {
	v := reflect.ValueOf(root)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		// must be a non-nil pointer to a struct
		return
	}
	visited := make(map[uintptr]struct{})
	_instantiateNilPointers(v, visited)
}

func _instantiateNilPointers(v reflect.Value, visited map[uintptr]struct{}) {
	// 1) Drill through pointers, but short-circuit on nil or already-seen
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return
		}
		addr := v.Pointer()
		if _, seen := visited[addr]; seen {
			// we’ve already initialized this struct – avoid a cycle
			return
		}
		visited[addr] = struct{}{}
		v = v.Elem()
	}

	// 2) Only structs from here on
	if v.Kind() != reflect.Struct {
		return
	}

	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		sf := t.Field(i)

		// skip unexported
		if sf.PkgPath != "" {
			continue
		}

		switch field.Kind() {
		case reflect.Ptr:
			// skip certain pointer types (e.g. time.Time, gorm.DeletedAt, etc.)
			if _, skip := skipTypes[field.Type()]; skip {
				continue
			}
			if field.IsNil() && field.CanSet() {
				// allocate the struct it points to
				field.Set(reflect.New(field.Type().Elem()))
			}
			// recurse into it
			_instantiateNilPointers(field, visited)

		case reflect.Struct:
			// recurse into embedded structs
			_instantiateNilPointers(field, visited)
		}
	}
}
