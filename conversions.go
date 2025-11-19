package oracle

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/cmmoran/go-ora/v2"
	"github.com/cmmoran/go-ora/v2/converters"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

var (
	tyTime   = reflect.TypeFor[time.Time]()
	ty16Byte = reflect.TypeFor[[16]byte]()
)

func convertToLiteral(stmt *gorm.Statement, val any, rv reflect.Value, f ...*schema.Field) any {
	var ret any
	rval, _, indirections := reflectValueDereference(val)
	if !rval.IsValid() {
		return val
	}
	v := rval.Interface()
	switch {
	case len(f) > 1 && (rval.Kind() == reflect.Slice || rval.Kind() == reflect.Array):
		ret = make([]any, 0)
		for i := 0; i < rval.Len(); i++ {
			v = convertToLiteral(stmt, rval.Index(i).Interface(), rv, f[i])
			ret = append(ret.([]any), v)
		}
		return ret.([]any)
	case len(f) == 1:
		field := f[0]
		switch rval.Type() {
		case tyTime:
			loc := stmt.DB.Dialector.(*Dialector).sessionLocation
			if loc == nil {
				loc = time.Local
			}
			prec := field.Precision
			if prec <= 0 || prec > 9 {
				prec = 6
			}

			vt, ok := v.(time.Time)
			if !ok {
				vt = time.Time{}
			}
			switch strings.ToLower(string(field.DataType)) {
			case "date":
				dr := reflect.ValueOf(converters.ToDate(vt, converters.WithLocation(loc)))
				for i := 0; i < indirections; i++ {
					dr, _ = reflectValueReference(dr.Interface(), true)
				}
				if err := field.Set(stmt.Context, rv, dr.Interface()); err != nil {
					return dr.Interface()
				}
				return dr.Interface()
			case "timestamp":
				dr := reflect.ValueOf(converters.ToTimestamp(vt, converters.WithLocation(loc), converters.WithPrecision(prec)))
				for i := 0; i < indirections; i++ {
					dr, _ = reflectValueReference(dr.Interface(), true)
				}
				if err := field.Set(stmt.Context, rv, dr.Interface()); err != nil {
					return dr.Interface()
				}
				return dr.Interface()
			case "timestamp with time zone":
				vt = trimFracTo(vt, prec)
				dr := reflect.ValueOf(vt)
				for i := 0; i < indirections; i++ {
					dr, _ = reflectValueReference(dr.Interface(), true)
				}
				if err := field.Set(stmt.Context, rv, dr.Interface()); err != nil {
					return dr.Interface()
				}
				return dr.Interface()
			case "timestamp with local time zone":
				dr := reflect.ValueOf(converters.ToTimestampWithLocalTimeZone(vt, converters.WithLocation(loc), converters.WithPrecision(prec)))
				for i := 0; i < indirections; i++ {
					dr, _ = reflectValueReference(dr.Interface(), true)
				}
				if err := field.Set(stmt.Context, rv, dr.Interface()); err != nil {
					return dr.Interface()
				}
				return dr.Interface()

			}
		case ty16Byte:
			return v.([]byte)[:]
		}
	}

	return val
}

func castValue(val any, dataType string, prec int, notnull bool) any {
	v, wasPtr := reflectDereference(val)
	if v == nil && wasPtr {
		return castNullExpr(dataType)
	}
	if v == nil {
		return nil
	}

	switch x := v.(type) {
	case bool:
		if x {
			return 1
		}
		return 0

	case string:
		if len(x) > 2000 {
			return go_ora.Clob{String: x, Valid: true}
		}
		if len(x) == 0 {
			if notnull {
				x = " "
			} else {
				return castNullExpr(dataType)
			}
		}
		return clause.Expr{
			SQL: fmt.Sprintf("CAST(? AS %s)", dataType),
			Vars: []any{
				x,
			},
		}

	case gorm.DeletedAt:
		if x.Valid && !x.Time.IsZero() {
			return x.Time
		}
		return sql.NullTime{}

	case time.Time:
		return castTime(x, dataType, prec)

	default:
		if reflect.TypeOf(x).ConvertibleTo(ty16Byte) {
			return castRaw16(x)
		}
		return x
	}
}

func castNullExpr(t string) any {
	if t == "" {
		return nil
	}
	t = strings.ToUpper(t)
	switch t {
	case "RAW(16)", "RAW(32)", "BLOB", "LONG RAW", "CHAR(1)", "VARCHAR2", "CLOB", "NCLOB",
		"NUMBER", "NUMBER(1)", "BINARY_FLOAT", "BINARY_DOUBLE", "FLOAT", "DATE", "TIMESTAMP",
		"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE", "INTERVAL YEAR TO MONTH",
		"INTERVAL DAY TO SECOND", "XMLTYPE", "JSON":
		return clause.Expr{SQL: fmt.Sprintf("CAST(NULL AS %s)", t)}
	default:
		if strings.HasPrefix(t, "VARCHAR2(") {
			return clause.Expr{SQL: fmt.Sprintf("CAST(NULL AS %s)", t)}
		}
		return nil
	}
}

func castTime(t time.Time, typ string, prec int) any {
	switch typ {
	case "DATE":
		return clause.Expr{
			SQL:  "CAST(TO_DATE(?, ?) AS DATE)",
			Vars: []any{t.Format("2006-01-02 15:04:05"), converters.NlsDateFormat},
		}
	case "TIMESTAMP":
		if prec > 0 {
			t = trimFracTo(t, prec)
			typ = fmt.Sprintf("%s(%d)", typ, prec)
		}
		return clause.Expr{
			SQL:  fmt.Sprintf("CAST(TO_TIMESTAMP(?, ?) AS %s)", typ),
			Vars: []any{t.Format("2006-01-02 15:04:05.999999999"), converters.NlsTimestampFormat},
		}
	case "TIMESTAMP WITH TIME ZONE":
		if prec > 0 {
			t = trimFracTo(t, prec)
			typ = fmt.Sprintf("TIMESTAMP(%d) WITH TIME ZONE", prec)
		}
		return clause.Expr{
			SQL:  fmt.Sprintf("CAST(TO_TIMESTAMP_TZ(?, ?) AS %s)", typ),
			Vars: []any{t.Format("2006-01-02 15:04:05.999999999-07:00"), converters.NlsTimestampTzFormat},
		}
	case "TIMESTAMP WITH LOCAL TIME ZONE":
		if prec > 0 {
			t = trimFracTo(t, prec)
			typ = fmt.Sprintf("TIMESTAMP(%d) WITH LOCAL TIME ZONE", prec)
		}
		return clause.Expr{
			SQL:  fmt.Sprintf("CAST(TO_TIMESTAMP_TZ(?, ?) AS %s)", typ),
			Vars: []any{t.Format("2006-01-02 15:04:05.999999999-07:00"), converters.NlsTimestampFormat},
		}
	default:
		return t
	}
}

func castRaw16(v any) any {
	b, ok := asRaw16(reflect.ValueOf(v))
	if !ok {
		return clause.Expr{SQL: "CAST(NULL AS RAW(16))"}
	}
	return clause.Expr{
		SQL:  "HEXTORAW(?)",
		Vars: []any{fmt.Sprintf("%x", b)},
	}
}

func trimFracTo(t time.Time, p int) time.Time {
	if p < 0 || p > 9 {
		return t
	}
	nanos := t.Nanosecond()
	scale := int(math.Pow10(9 - p))
	rounded := (nanos + scale/2) / scale * scale
	return time.Date(t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), rounded, t.Location())
}

// asRaw16 returns a 16-byte slice if v is any T or *T whose underlying type is [16]byte,
// or a []byte of length 16, or a UUID string in canonical form.
func asRaw16(v reflect.Value) ([]byte, bool) {
	// Fully unwrap interface and pointer layers
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr) {
		if v.IsNil() {
			return nil, true // NULL
		}
		v = v.Elem()
	}

	// [16]byte or named type with underlying [16]byte
	if v.IsValid() && v.Kind() == reflect.Array && v.Len() == 16 && v.Type().Elem().Kind() == reflect.Uint8 {
		b := make([]byte, 16)
		for i := 0; i < 16; i++ {
			b[i] = byte(v.Index(i).Uint())
		}
		return b, true
	}

	// UUID-ish string (with or without '-')
	if v.IsValid() && v.Kind() == reflect.String {
		s := v.String()
		// Remove hyphens if present
		if strings.ContainsRune(s, '-') {
			buf := make([]byte, 0, 32)
			for i := 0; i < len(s); i++ {
				if s[i] != '-' {
					buf = append(buf, s[i])
				}
			}
			s = string(buf)
		}

		if len(s) == 32 {
			out := make([]byte, 16)
			for i := 0; i < 16; i++ {
				h1, ok1 := fromHex(s[i*2])
				h2, ok2 := fromHex(s[i*2+1])
				if !ok1 || !ok2 {
					goto notuuid
				}
				out[i] = (h1 << 4) | h2
			}
			return out, true
		}
	}
notuuid:
	return nil, false
}

func fromHex(r byte) (byte, bool) {
	switch {
	case '0' <= r && r <= '9':
		return r - '0', true
	case 'a' <= r && r <= 'f':
		return r - 'a' + 10, true
	case 'A' <= r && r <= 'F':
		return r - 'A' + 10, true
	}
	return 0, false
}
