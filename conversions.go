package oracle

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/cmmoran/go-ora/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func convertValue(val any, dataType string, prec int, notnull bool) any {
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
		return convertTime(x, dataType, prec)

	default:
		if reflect.TypeOf(x).ConvertibleTo(ty16Byte) {
			return convertRaw16(x)
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

func convertTime(t time.Time, typ string, prec int) any {
	switch typ {
	case "DATE":
		return clause.Expr{
			SQL: "CAST(TO_DATE(?, ?) AS DATE)",
			Vars: []any{
				t.Format("2006-01-02T15:04:05"),
				`YYYY-MM-DD"T"HH24:MI:SS`},
		}
	case "TIMESTAMP":
		if prec > 0 {
			t = trimFracTo(t, prec)
			typ = fmt.Sprintf("%s(%d)", typ, prec)
		}
		return clause.Expr{
			SQL:  fmt.Sprintf("CAST(TO_TIMESTAMP(?, ?) AS %s)", typ),
			Vars: []any{t.Format("2006-01-02T15:04:05.999999"), `YYYY-MM-DD"T"HH24:MI:SS.FF6`},
		}
	case "TIMESTAMP WITH TIME ZONE":
		if prec > 0 {
			t = trimFracTo(t, prec)
			typ = fmt.Sprintf("TIMESTAMP(%d) WITH TIME ZONE", prec)
		}
		return clause.Expr{
			SQL:  fmt.Sprintf("CAST(TO_TIMESTAMP_TZ(?, ?) AS %s)", typ),
			Vars: []any{t.Format("2006-01-02T15:04:05.999999-07:00"), `YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM`},
		}
	case "TIMESTAMP WITH LOCAL TIME ZONE":
		if prec > 0 {
			t = trimFracTo(t, prec)
			typ = fmt.Sprintf("TIMESTAMP(%d) WITH LOCAL TIME ZONE", prec)
		}
		return clause.Expr{
			SQL:  fmt.Sprintf("CAST(TO_TIMESTAMP_TZ(?, ?) AS %s)", typ),
			Vars: []any{t.Format("2006-01-02T15:04:05.999999-07:00"), `YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM`},
		}
	default:
		return t
	}
}

func convertRaw16(v any) any {
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
