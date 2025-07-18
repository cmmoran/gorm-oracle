package oracle

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	go_ora "github.com/sijms/go-ora/v2"

	"gorm.io/gorm/utils"
)

const (
	tmFmtWithMicroTz = "2006-01-02 15:04:05.999999999Z07:00"
	tmFmtWithMicro   = "2006-01-02 15:04:05.999999999"
	tmFmtWithMS      = "2006-01-02 15:04:05.999"
	tmFmtZero        = "0000-00-00 00:00:00.000000000"
	nullStr          = "NULL"
)

func isPrintable(s string) bool {
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

// A list of Go types that should be converted to SQL primitives
var convertibleTypes = []reflect.Type{reflect.TypeOf(time.Time{}), reflect.TypeOf(false), reflect.TypeOf([]byte{})}

// RegEx matches only numeric values
var numericPlaceholderRe = regexp.MustCompile(`\$\d+\$`)

func isNumeric(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// ExplainSQL generate SQL string with given parameters, the generated SQL is expected to be used in logger, execute it might introduce a SQL injection vulnerability
func ExplainSQL(sql string, numericPlaceholder *regexp.Regexp, escaper string, avars ...interface{}) string {
	var (
		convertParams func(interface{}, int)
		vars          = make([]string, len(avars))
	)

	convertParams = func(v interface{}, idx int) {
		switch v := v.(type) {
		case bool:
			vars[idx] = strconv.FormatBool(v)
		case time.Time:
			if v.IsZero() {
				vars[idx] = escaper + tmFmtZero + escaper
			} else {
				vars[idx] = escaper + v.Format(tmFmtWithMicroTz) + escaper
			}
		case *time.Time:
			if v != nil {
				if v.IsZero() {
					vars[idx] = escaper + tmFmtZero + escaper
				} else {
					vars[idx] = escaper + v.Format(tmFmtWithMicroTz) + escaper
				}
			} else {
				vars[idx] = nullStr
			}
		case go_ora.Out:
			convertParams(v.Dest, idx)
			if v.Dest != nil {
				str := fmt.Sprintf("%v", v.Dest)
				if tstr, ok := v.Dest.(time.Time); ok {
					str = tstr.Format(tmFmtWithMicroTz)
				}
				if tstr, ok := v.Dest.(*time.Time); ok {
					str = tstr.Format(tmFmtWithMicroTz)
				}
				if v.Size > 0 {
					vars[idx] = escaper + fmt.Sprintf(" /*-go_ora.Out{Dest:%s,Size:%d}-*/", str, v.Size) + escaper
				} else {
					vars[idx] = escaper + fmt.Sprintf(" /*-go_ora.Out{Dest:%s}-*/", str) + escaper
				}
			}
		case driver.Valuer:
			reflectValue := reflect.ValueOf(v)
			if v != nil && reflectValue.IsValid() && ((reflectValue.Kind() == reflect.Ptr && !reflectValue.IsNil()) || reflectValue.Kind() != reflect.Ptr) {
				r, _ := v.Value()
				if r == nil {
					vars[idx] = nullStr
					return
				}
				convertParams(r, idx)
			} else {
				vars[idx] = nullStr
			}
		case fmt.Stringer:
			reflectValue := reflect.ValueOf(v)
			switch reflectValue.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				vars[idx] = fmt.Sprintf("%d", reflectValue.Interface())
			case reflect.Float32, reflect.Float64:
				vars[idx] = fmt.Sprintf("%.6f", reflectValue.Interface())
			case reflect.Bool:
				vars[idx] = fmt.Sprintf("%t", reflectValue.Interface())
			case reflect.String:
				vars[idx] = escaper + strings.ReplaceAll(fmt.Sprintf("%v", v), escaper, escaper+escaper) + escaper
			default:
				if v != nil && reflectValue.IsValid() && ((reflectValue.Kind() == reflect.Ptr && !reflectValue.IsNil()) || reflectValue.Kind() != reflect.Ptr) {
					vars[idx] = escaper + strings.ReplaceAll(fmt.Sprintf("%v", v), escaper, escaper+escaper) + escaper
				} else {
					vars[idx] = nullStr
				}
			}
		case []byte:
			if len(v) == 16 { // @HACK: handle UUID-ish values
				vars[idx] = escaper + hex.EncodeToString(v[:]) + escaper
			} else {
				if s := string(v); isPrintable(s) {
					vars[idx] = escaper + strings.ReplaceAll(s, escaper, escaper+escaper) + escaper
				} else {
					vars[idx] = escaper + fmt.Sprintf("<binary:%s>", hex.EncodeToString(v[:])) + escaper
				}
			}
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			vars[idx] = utils.ToString(v)
		case float32:
			vars[idx] = strconv.FormatFloat(float64(v), 'f', -1, 32)
		case float64:
			vars[idx] = strconv.FormatFloat(v, 'f', -1, 64)
		case string:
			vars[idx] = escaper + strings.ReplaceAll(v, escaper, escaper+escaper) + escaper
		default:
			rv := reflect.ValueOf(v)
			if v == nil || !rv.IsValid() || rv.Kind() == reflect.Ptr && rv.IsNil() {
				vars[idx] = nullStr
			} else if valuer, ok := v.(driver.Valuer); ok {
				v, _ = valuer.Value()
				convertParams(v, idx)
			} else if rv.Kind() == reflect.Ptr && !rv.IsZero() {
				convertParams(reflect.Indirect(rv).Interface(), idx)
			} else if isNumeric(rv.Kind()) {
				if rv.CanInt() || rv.CanUint() {
					vars[idx] = fmt.Sprintf("%d", rv.Interface())
				} else {
					vars[idx] = fmt.Sprintf("%.6f", rv.Interface())
				}
			} else {
				for _, t := range convertibleTypes {
					if rv.Type().ConvertibleTo(t) {
						convertParams(rv.Convert(t).Interface(), idx)
						return
					}
				}
				vars[idx] = escaper + strings.ReplaceAll(fmt.Sprint(v), escaper, escaper+escaper) + escaper
			}
		}
	}

	for idx, v := range avars {
		convertParams(v, idx)
	}

	if numericPlaceholder == nil {
		var idx int
		var newSQL strings.Builder

		for _, v := range []byte(sql) {
			if v == '?' {
				if len(vars) > idx {
					newSQL.WriteString(vars[idx])
					idx++
					continue
				}
			}
			newSQL.WriteByte(v)
		}

		sql = newSQL.String()
	} else {
		sql = numericPlaceholder.ReplaceAllString(sql, "$$$1$$")

		sql = numericPlaceholderRe.ReplaceAllStringFunc(sql, func(v string) string {
			num := v[1 : len(v)-1]
			n, _ := strconv.Atoi(num)

			// position var start from 1 ($1, $2)
			n -= 1
			if n >= 0 && n <= len(vars)-1 {
				return vars[n]
			}
			return v
		})
	}

	return sql
}
