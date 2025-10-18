package oracle

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/sijms/go-ora/v2"
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
