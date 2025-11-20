package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cmmoran/go-ora/v2"
	"github.com/cmmoran/go-ora/v2/converters"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Config struct {
	DriverName        string
	DSN               string
	Conn              gorm.ConnPool //*sql.DB
	DefaultStringSize uint
	DBVer             string

	// IgnoreCase applies to data; not identifiers
	IgnoreCase bool // warning: may cause performance issues
	// NamingCaseSensitive applies to identifiers
	NamingCaseSensitive bool // whether naming is case-sensitive
	// PreferredCase determines the strategy for naming identifiers; Note that setting PreferredCase to CamelCase or SnakeCase will override the NamingCaseSensitive setting; ScreamingSnakeCase is the default and works with both case-sensitive and case-insensitive naming
	PreferredCase Case

	// whether VARCHAR type size is character length, defaulting to byte length
	VarcharSizeIsCharLength bool

	// RowNumberAliasForOracle11 is the alias for ROW_NUMBER() in Oracle 11g, defaulting to ROW_NUM
	RowNumberAliasForOracle11 string
	UseClobForTextType        bool
	// time conversion for all clauses to ensure proper time rounding
	TimeGranularity time.Duration
	// use this timezone for the session
	SessionTimezone string
	sessionLocation *time.Location

	namingStrategy *NamingStrategy
}

// Dialector implement GORM database dialector
type Dialector struct {
	*Config
}

//goland:noinspection GoUnusedExportedFunction
func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

//goland:noinspection GoUnusedExportedFunction
func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

// BuildUrl create databaseURL from server, port, service, user, password, urlOptions
// this function help build a will formed databaseURL and accept any character as it
// convert special charters to corresponding values in URL
//
//goland:noinspection GoUnusedExportedFunction
func BuildUrl(server string, port int, service, user, password string, options map[string]string) string {
	return go_ora.BuildUrl(server, port, service, user, password, options)
}

// GetStringExpr replace single quotes in the string with two single quotes
// and return the expression for the string value
//
//	quotes : if the SQL placeholder is ? then pass true, if it is '?' then do not pass or pass false.
func GetStringExpr(value string, quotes ...bool) clause.Expr {
	if len(quotes) > 0 && quotes[0] {
		if strings.Contains(value, "'") {
			// escape single quotes
			if !strings.Contains(value, "]'") {
				value = fmt.Sprintf("q'[%s]'", value)
			} else if !strings.Contains(value, "}'") {
				value = fmt.Sprintf("q'{%s}'", value)
			} else if !strings.Contains(value, ">'") {
				value = fmt.Sprintf("q'<%s>'", value)
			} else if !strings.Contains(value, ")'") {
				value = fmt.Sprintf("q'(%s)'", value)
			} else {
				value = fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''"))
			}
		} else {
			value = fmt.Sprintf("'%s'", value)
		}
	} else {
		value = strings.ReplaceAll(value, "'", "''")
	}
	return gorm.Expr(value)
}

// AddSessionParams setting database connection session parameters,
// the value is wrapped in single quotes.
//
// If the value doesn't need to be wrapped in single quotes,
// please use the go_ora.AddSessionParam function directly,
// or pass the originals parameter as true.
func AddSessionParams(db *sql.DB, params map[string]string, originals ...bool) (keys []string, err error) {
	if db == nil {
		return
	}
	if _, ok := db.Driver().(*go_ora.OracleDriver); !ok {
		return
	}
	var original bool
	if len(originals) > 0 {
		original = originals[0]
	}

	for key, value := range params {
		if key == "" || value == "" {
			continue
		}
		if !original {
			value = GetStringExpr(value, true).SQL
		}
		if err = go_ora.AddSessionParam(db, key, value); err != nil {
			return
		}
		keys = append(keys, key)
	}
	return
}

// DelSessionParams remove session parameters
func DelSessionParams(db *sql.DB, keys []string) {
	if db == nil {
		return
	}
	if _, ok := db.Driver().(*go_ora.OracleDriver); !ok {
		return
	}

	for _, key := range keys {
		if key == "" {
			continue
		}
		go_ora.DelSessionParam(db, key)
	}
}

func reflectDereference(obj any) (any, bool) {
	if obj == nil {
		return nil, false
	}

	var (
		v  reflect.Value
		ok bool
	)
	if v, ok = obj.(reflect.Value); !ok {
		v = reflect.ValueOf(obj)
	}

	if !v.IsValid() {
		return nil, false
	}

	isPtr := false
	// Unwrap interfaces and pointers
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil, true
		}
		v = v.Elem()
		isPtr = true
	}

	return v.Interface(), isPtr
}

func reflectValueDereference(obj any) (reflect.Value, bool, int) {
	if obj == nil {
		return reflect.ValueOf(obj), false, 0
	}

	var (
		v  reflect.Value
		ok bool
	)
	if v, ok = obj.(reflect.Value); !ok {
		v = reflect.ValueOf(obj)
	}

	if !v.IsValid() {
		return v, false, 0
	}

	isPtr := false
	indirections := 0
	// Unwrap interfaces and pointers
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return v, true, 0
		}
		v = v.Elem()
		isPtr = true
		indirections++
	}

	return v, isPtr, indirections
}

func reflectReference(obj any, wrapPointers ...bool) any {
	if obj == nil {
		return nil
	}

	var (
		v  reflect.Value
		ok bool
	)
	if v, ok = obj.(reflect.Value); !ok {
		v = reflect.ValueOf(obj)
	}

	// Unwrap interfaces
	for v.Kind() == reflect.Interface && !v.IsNil() {
		v = v.Elem()
	}

	// Decide whether to wrap pointers or not
	if v.Kind() == reflect.Ptr {
		if len(wrapPointers) == 0 || !wrapPointers[0] {
			return obj // Leave pointer as-is
		}
		// wrapPointers[0] is true → wrap pointer again
	}

	// Create a new pointer to the value
	ptrVal := reflect.New(v.Type())
	ptrVal.Elem().Set(v)

	return ptrVal.Interface()
}

func reflectReferenceDepth(obj any, depth int) any {
	if obj == nil {
		return nil
	}

	var (
		v  reflect.Value
		ok bool
	)
	if v, ok = obj.(reflect.Value); !ok {
		v = reflect.ValueOf(obj)
	}

	// Unwrap interfaces
	for v.Kind() == reflect.Interface && !v.IsNil() {
		v = v.Elem()
	}

	// Decide whether to wrap pointers or not
	if v.Kind() == reflect.Ptr {
		if depth == 0 {
			return obj // Leave pointer as-is
		}
	}

	// Create a new pointer to the value
	ptrVal := reflect.New(v.Type())
	ptrVal.Elem().Set(v)

	if depth == 0 {
		return ptrVal.Interface()
	}
	return reflectReferenceDepth(ptrVal.Interface(), depth-1)
}

func reflectValueReference(obj any, wrapPointers ...bool) (reflect.Value, bool) {
	if obj == nil {
		return reflect.ValueOf(obj), false
	}

	var (
		v  reflect.Value
		ok bool
	)
	if v, ok = obj.(reflect.Value); !ok {
		v = reflect.ValueOf(obj)
	}

	// Unwrap interfaces
	for v.Kind() == reflect.Interface && !v.IsNil() {
		v = v.Elem()
	}

	// Decide whether to wrap pointers or not
	if v.Kind() == reflect.Ptr {
		if len(wrapPointers) == 0 || !wrapPointers[0] {
			return reflect.ValueOf(obj), true // Leave pointer as-is
		}
		// wrapPointers[0] is true → wrap pointer again
	}

	// Create a new pointer to the value
	ptrVal := reflect.New(v.Type())
	ptrVal.Elem().Set(v)

	return ptrVal, true
}

func reflectValueReferenceDepth(obj any, depth int) (reflect.Value, bool) {
	if obj == nil {
		return reflect.ValueOf(obj), false
	}
	var (
		v  reflect.Value
		ok bool
	)
	if v, ok = obj.(reflect.Value); !ok {
		v = reflect.ValueOf(obj)
	}

	// Unwrap interfaces
	for v.Kind() == reflect.Interface && !v.IsNil() {
		v = v.Elem()
	}

	// Decide whether to wrap pointers or not
	if v.Kind() == reflect.Ptr {
		if depth == 0 {
			return v, true // Leave pointer as-is
		}
	}

	// Create a new pointer to the value
	ptrVal := reflect.New(v.Type())
	ptrVal.Elem().Set(v)

	if depth == 0 {
		return ptrVal, true
	}
	return reflectValueReferenceDepth(ptrVal, depth-1)
}

func (d Dialector) DummyTableName() string {
	return "DUAL"
}

func (d Dialector) Name() string {
	return "oracle"
}

func (d Dialector) Initialize(db *gorm.DB) (err error) {
	if d.PreferredCase == SnakeCase || d.PreferredCase == CamelCase {
		d.NamingCaseSensitive = true
	}
	d.namingStrategy = &NamingStrategy{
		NamingCaseSensitive: d.NamingCaseSensitive,
		PreferredCase:       d.PreferredCase,
	}
	db.NamingStrategy = d.namingStrategy

	d.DefaultStringSize = 1024

	// register callbacks
	config := &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES", "ON CONFLICT", "RETURNING"},
		UpdateClauses: []string{"UPDATE", "SET", "WHERE", "RETURNING"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE", "RETURNING"},
		QueryClauses:  []string{"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "FOR"},
	}
	callbacks.RegisterDefaultCallbacks(db, config)

	d.DriverName = "oracle"

	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		db.ConnPool, err = sql.Open(d.DriverName, d.DSN)
		if err != nil {
			return
		}
	}
	if d.IgnoreCase {
		if sqlDB, ok := db.ConnPool.(*sql.DB); ok {
			// warning: may cause performance issues
			_ = go_ora.AddSessionParam(sqlDB, "NLS_COMP", "LINGUISTIC")
			_ = go_ora.AddSessionParam(sqlDB, "NLS_SORT", "BINARY_CI")
		}
	}

	loc, err := time.LoadLocation(d.SessionTimezone)
	if err != nil {
		loc = time.Local
	}
	d.sessionLocation = loc
	if sqlDB, ok := db.ConnPool.(*sql.DB); ok {
		_, _ = AddSessionParams(sqlDB, map[string]string{
			"TIME_ZONE":               loc.String(),
			"NLS_DATE_FORMAT":         converters.NlsDateFormat,
			"NLS_TIMESTAMP_FORMAT":    converters.NlsTimestampFormat,
			"NLS_TIMESTAMP_TZ_FORMAT": converters.NlsTimestampTzFormat,
			"NLS_TIME_FORMAT":         converters.NlsTimeFormat,
			"NLS_TIME_TZ_FORMAT":      converters.NlsTimeTzFormat,
		})
	}

	err = db.ConnPool.QueryRowContext(context.Background(), "select version from product_component_version where rownum = 1").Scan(&d.DBVer)
	if err != nil {
		return err
	}

	d.namingStrategy.capIdentifierMaxLength = 30
	// https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Database-Object-Names-and-Qualifiers.html
	dbverSplits := strings.Split(d.DBVer, ".")
	if dbVer, _ := strconv.Atoi(dbverSplits[0]); dbVer == 12 {
		if dbMinor, _ := strconv.Atoi(dbverSplits[1]); dbMinor >= 2 {
			d.namingStrategy.capIdentifierMaxLength = 128
		}
	} else if dbVer > 12 {
		d.namingStrategy.capIdentifierMaxLength = 128
	}
	if err = db.Callback().Create().Replace("gorm:create", Create); err != nil {
		return
	}
	if err = db.Callback().Update().Replace("gorm:update", Update); err != nil {
		return
	}
	if err = db.Callback().Delete().Replace("gorm:delete", Delete); err != nil {
		return
	}
	if err = db.Callback().Query().Replace("gorm:query", Query); err != nil {
		return
	}

	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return
}

func (d Dialector) ClauseBuilders() (clauseBuilders map[string]clause.ClauseBuilder) {
	clauseBuilders = make(map[string]clause.ClauseBuilder)

	if dbVer, _ := strconv.Atoi(strings.Split(d.DBVer, ".")[0]); dbVer > 11 {
		clauseBuilders["LIMIT"] = d.RewriteLimit
	} else {
		clauseBuilders["LIMIT"] = d.RewriteLimit11
	}

	clauseBuilders["RETURNING"] = func(c clause.Clause, builder clause.Builder) {
		if _, ok := c.Expression.(Returning); ok {
			c.Build(builder)
		}
		if cret, ok := c.Expression.(clause.Returning); ok {
			stmt, _ := builder.(*gorm.Statement)
			if len(cret.Columns) > 0 {
				c.Expression = ReturningWithColumns(cret.Columns)
			} else {
				c.Expression = Returning{}
			}
			stmt.Clauses["RETURNING"] = c
			c.Build(builder)
		}
	}
	clauseBuilders["WHERE"] = func(c clause.Clause, builder clause.Builder) {
		stmt, _ := builder.(*gorm.Statement)
		if stmt.Schema != nil {
			for i, ws := range c.Expression.(clause.Where).Exprs {
				switch wst := ws.(type) {
				case clause.IN:
					in := wst // the IN expression in the Exprs list
					values := in.Values
					n := len(values)

					if n <= 1000 {
						continue
					}

					// rewrite the IN into a chain of OR(IN-chunk)
					chunks := chunk(values, 1000)

					// build list of OR operands
					orExprs := make([]clause.Expression, len(chunks))
					for ci, chk := range chunks {
						orExprs[ci] = clause.IN{
							Column: in.Column,
							Values: chk,
						}
					}

					// Replace the IN expression with an OR expression
					c.Expression.(clause.Where).Exprs[i] = clause.Or(orExprs...)

					// Important: write back the updated Where clause into stmt so the builder sees it
					stmt.Clauses["WHERE"] = c
				case clause.Eq:
					name := ""
					if ccol, cok := wst.Column.(clause.Column); cok {
						name = ccol.Name
					} else if scol, sok := wst.Column.(string); sok {
						name = scol
					}

					if f := stmt.Schema.LookUpField(name); f != nil {
						c.Expression.(clause.Where).Exprs[i] = clause.Eq{
							Column: clause.Column{Table: stmt.Table, Name: f.DBName},
							Value:  convertToLiteral(stmt, wst.Value, stmt.ReflectValue, f),
						}
						stmt.Clauses["WHERE"] = c
					}
				case clause.Expr:
					if strings.Contains(wst.SQL, "=") {
						sp := strings.Split(wst.SQL, "=")
						k := sp[0]
						if name, ok := IsExplicitQuoted(k); ok {
							k = name
						}
						if f := stmt.Schema.LookUpField(k); f != nil {
							wst.Vars[0] = convertToLiteral(stmt, wst.Vars[0], stmt.ReflectValue, f)
							c.Expression.(clause.Where).Exprs[i] = clause.Expr{
								SQL:                wst.SQL,
								Vars:               wst.Vars,
								WithoutParentheses: wst.WithoutParentheses,
							}
						}
					}
				}
			}
		}
		c.Build(builder)
	}
	return
}

func chunk[T any](in []T, n int) [][]T {
	var out [][]T
	for len(in) > n {
		out = append(out, in[:n])
		in = in[n:]
	}
	if len(in) > 0 {
		out = append(out, in)
	}
	return out
}

func (d Dialector) getLimitRows(limit clause.Limit) (limitRows int, hasLimit bool) {
	if l := limit.Limit; l != nil {
		limitRows = *l
		hasLimit = limitRows > 0
	}
	return
}

func (d Dialector) RewriteLimit(c clause.Clause, builder clause.Builder) {
	if limit, ok := c.Expression.(clause.Limit); ok {
		limitRows, hasLimit := d.getLimitRows(limit)

		if stmt, ok := builder.(*gorm.Statement); ok {
			if _, hasOrderBy := stmt.Clauses["ORDER BY"]; !hasOrderBy && hasLimit {
				s := stmt.Schema
				_, _ = builder.WriteString("ORDER BY ")
				if s != nil && s.PrioritizedPrimaryField != nil {
					builder.WriteQuoted(s.PrioritizedPrimaryField.DBName)
					_ = builder.WriteByte(' ')
				} else {
					_, _ = builder.WriteString("(SELECT NULL FROM ")
					_, _ = builder.WriteString(d.DummyTableName())
					_, _ = builder.WriteString(")")
				}
			}
		}

		if offset := limit.Offset; offset > 0 {
			_, _ = builder.WriteString(" OFFSET ")
			builder.AddVar(builder, offset)
			_, _ = builder.WriteString(" ROWS")
		}
		if hasLimit {
			_, _ = builder.WriteString(" FETCH NEXT ")
			builder.AddVar(builder, limitRows)
			_, _ = builder.WriteString(" ROWS ONLY")
		}
	}
}

// RewriteLimit11 rewrite the LIMIT clause in the query to accommodate pagination requirements for Oracle 11g and lower database versions
//
// # Limit and Offset
//
//	SELECT * FROM (SELECT T.*, ROW_NUMBER() OVER (ORDER BY column) AS ROW_NUM FROM table_name T)
//	WHERE ROW_NUM BETWEEN offset+1 AND offset+limit
//
// # Only Limit
//
//	SELECT * FROM table_name WHERE ROWNUM <= limit ORDER BY column
//
// # Only Offset
//
//	SELECT * FROM table_name WHERE ROWNUM > offset ORDER BY column
func (d Dialector) RewriteLimit11(c clause.Clause, builder clause.Builder) {
	limit, ok := c.Expression.(clause.Limit)
	if !ok {
		return
	}
	offsetRows := limit.Offset
	hasOffset := offsetRows > 0
	limitRows, hasLimit := d.getLimitRows(limit)
	if !hasOffset && !hasLimit {
		return
	}

	var stmt *gorm.Statement
	if stmt, ok = builder.(*gorm.Statement); !ok {
		return
	}

	if hasLimit && hasOffset {
		// Implementing pagination queries using ROW_NUMBER() and subqueries
		if d.RowNumberAliasForOracle11 == "" {
			d.RowNumberAliasForOracle11 = "ROW_NUM"
		}
		subQuerySQL := fmt.Sprintf(
			"SELECT * FROM (SELECT T.*, ROW_NUMBER() OVER (ORDER BY %s) AS %s FROM (%s) T) WHERE %s BETWEEN %d AND %d",
			d.getOrderByColumns(stmt),
			d.RowNumberAliasForOracle11,
			strings.TrimSpace(stmt.SQL.String()),
			d.RowNumberAliasForOracle11,
			offsetRows+1,
			offsetRows+limitRows,
		)
		stmt.SQL.Reset()
		stmt.SQL.WriteString(subQuerySQL)
	} else if hasLimit {
		d.rewriteRownumStmt(stmt, builder, " <= ", limitRows)
	} else {
		d.rewriteRownumStmt(stmt, builder, " > ", offsetRows)
	}
}

func (d Dialector) rewriteRownumStmt(stmt *gorm.Statement, builder clause.Builder, operator string, rows int) {
	limitSql := strings.Builder{}
	if _, ok := stmt.Clauses["WHERE"]; !ok {
		limitSql.WriteString(" WHERE ")
	} else {
		limitSql.WriteString(" AND ")
	}
	limitSql.WriteString("ROWNUM")
	limitSql.WriteString(operator)
	limitSql.WriteString(strconv.Itoa(rows))

	if _, hasOrderBy := stmt.Clauses["ORDER BY"]; !hasOrderBy {
		_, _ = builder.WriteString(limitSql.String())
	} else {
		// "ORDER BY" before insert
		sqlTmp := strings.Builder{}
		sqlOld := stmt.SQL.String()
		orderIndex := strings.Index(sqlOld, "ORDER BY") - 1
		sqlTmp.WriteString(sqlOld[:orderIndex])
		sqlTmp.WriteString(limitSql.String())
		sqlTmp.WriteString(sqlOld[orderIndex:])
		stmt.SQL = sqlTmp
	}
}

func (d Dialector) getOrderByColumns(stmt *gorm.Statement) string {
	if orderByClause, ok := stmt.Clauses["ORDER BY"]; ok {
		var orderBy clause.OrderBy
		if orderBy, ok = orderByClause.Expression.(clause.OrderBy); ok && len(orderBy.Columns) > 0 {
			orderByBuilder := strings.Builder{}
			for i, column := range orderBy.Columns {
				if i > 0 {
					orderByBuilder.WriteString(", ")
				}
				orderByBuilder.WriteString(column.Column.Name)
				if column.Desc {
					orderByBuilder.WriteString(" DESC")
				}
			}
			return orderByBuilder.String()
		}
	}
	return "NULL"
}

func (d Dialector) DefaultValueOf(*schema.Field) clause.Expression {
	return clause.Expr{SQL: "VALUES (DEFAULT)"}
}

func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:                          db,
				Dialector:                   d,
				CreateIndexAfterCreateTable: true,
			},
		},
		namingStrategy: d.namingStrategy,
	}
}

func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, _ interface{}) {
	_, _ = writer.WriteString(":")
	_, _ = writer.WriteString(strconv.Itoa(len(stmt.Vars)))
}

// QuoteTo writes a SQL-quoted identifier (or dotted path) to writer.
// When NamingCaseSensitive is true, every dot-separated part is wrapped
// in double quotes and any internal `"` are escaped as `""`.
// Existing outer quotes around parts are normalized (removed then re-applied).
func (d Dialector) QuoteTo(w clause.Writer, s string) {
	_, _ = w.WriteString(d.namingStrategy.normalizeQualified(s))
}

var numericPlaceholder = regexp.MustCompile(`:(\d+)`)

func (d Dialector) Explain(sql string, vars ...interface{}) string {
	for idx, val := range vars {
		vv, _ := reflectDereference(val)
		switch v := vv.(type) {
		case bool:
			if v {
				vars[idx] = 1
			} else {
				vars[idx] = 0
			}
		case go_ora.Clob:
			vars[idx] = v.String
		}
	}
	return ExplainSQL(sql, numericPlaceholder, `'`, vars...)
}

// Check for types that match ~[16]byte
func isSixteenByteType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.ConvertibleTo(ty16Byte)
}

func (d Dialector) DataTypeOf(field *schema.Field) string {
	delete(field.TagSettings, "RESTRICT")

	// Handle any uuid/ulid as RAW(16)
	if isSixteenByteType(field.FieldType) {
		return "RAW(16)"
	}

	var sqlType string
	switch field.DataType {
	case schema.Bool:
		booleanType := "NUMBER(1)"
		if len(d.DBVer) > 0 {
			if dbVer, _ := strconv.Atoi(strings.Split(d.DBVer, ".")[0]); dbVer >= 23 {
				booleanType = "BOOLEAN"
			}
		}
		sqlType = booleanType
	case schema.Int, schema.Uint:
		sqlType = "INTEGER"
		if field.Size > 0 && field.Size <= 8 {
			sqlType = "SMALLINT"
		}

		if field.AutoIncrement {
			sqlType += " GENERATED BY DEFAULT AS IDENTITY"
		}
	case schema.Float:
		sqlType = "FLOAT"
	case schema.String, "VARCHAR2", "varchar2":
		size := field.Size
		defaultSize := d.DefaultStringSize

		if size == 0 {
			if defaultSize > 0 {
				size = int(defaultSize)
			} else {
				hasIndex := field.TagSettings["INDEX"] != "" || field.TagSettings["UNIQUE"] != ""
				// TEXT, GEOMETRY or JSON column can't have a default value
				if field.PrimaryKey || field.HasDefaultValue || hasIndex {
					size = 191 // utf8mb4
				}
			}
		}

		if size > 0 && size <= 4000 {
			// By default, VARCHAR2 can specify a positive integer no larger than 4000 as its byte length.
			if d.VarcharSizeIsCharLength {
				if size*3 > 4000 {
					sqlType = "CLOB"
				} else {
					// Character length（size * 3）
					sqlType = fmt.Sprintf("VARCHAR2(%d CHAR)", size)
				}
			} else {
				sqlType = fmt.Sprintf("VARCHAR2(%d)", size)
			}
		} else {
			if d.Config.UseClobForTextType {
				sqlType = "CLOB"
			} else {
				if field.Size > 0 {
					sqlType = fmt.Sprintf("VARCHAR2(%d)", field.Size)
				} else {
					sqlType = "VARCHAR2(4000)"
				}
			}
		}
	case schema.Time, "timestamp with time zone":
		if field.Precision > 0 && field.Precision <= 9 {
			sqlType = fmt.Sprintf("TIMESTAMP(%d) WITH TIME ZONE", field.Precision)
		} else {
			sqlType = "TIMESTAMP WITH TIME ZONE"
		}
	case schema.Bytes:
		sqlType = "BLOB"
	case "timestamp without time zone":
		if field.Precision > 0 && field.Precision <= 9 {
			sqlType = fmt.Sprintf("TIMESTAMP(%d) WITH LOCAL TIME ZONE", field.Precision)
		} else {
			sqlType = "TIMESTAMP WITH LOCAL TIME ZONE"
		}
	case "timestamp":
		if field.Precision > 0 && field.Precision <= 9 {
			sqlType = fmt.Sprintf("TIMESTAMP(%d)", field.Precision)
		} else {
			sqlType = "TIMESTAMP"
		}
	case "date":
		sqlType = "DATE"
	default:
		sqlType = string(field.DataType)

		if strings.EqualFold(sqlType, "text") {
			if d.Config.UseClobForTextType {
				sqlType = "CLOB"
			} else {
				if field.Size > 0 {
					sqlType = fmt.Sprintf("VARCHAR2(%d)", field.Size)
				} else {
					sqlType = "VARCHAR2(4000)"
				}
			}
		}

		if sqlType == "" {
			panic(fmt.Sprintf("invalid sql type %s (%s) for oracle", field.FieldType.Name(), field.FieldType.String()))
		}
	}

	return sqlType
}

func (d Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVEPOINT " + name)
	return tx.Error
}

func (d Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	return tx.Error
}

func (d Dialector) Translate(err error) error {
	if err == nil {
		return err
	}
	if strings.Contains(err.Error(), "output parameter should be pointer type") {
		var terr error
		if e, ok := err.(interface{ Unwrap() error }); ok {
			terr = e.Unwrap()
		}
		if e, ok := err.(interface{ Unwrap() []error }); ok {
			terrs := e.Unwrap()[1:]
			terr = errors.Join(terrs...)
		}
		return terr
	}
	return err
}
