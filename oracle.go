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

	oracle "github.com/godoes/gorm-oracle"
	"github.com/sijms/go-ora/v2"
	"golang.org/x/exp/slices"
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

	v := reflect.ValueOf(obj)

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

func reflectReference(obj any, wrapPointers ...bool) any {
	if obj == nil {
		return nil
	}

	v := reflect.ValueOf(obj)

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
		_, _ = oracle.AddSessionParams(sqlDB, map[string]string{
			"TIME_ZONE":               loc.String(),
			"NLS_DATE_FORMAT":         `YYYY-MM-DD"T"HH24:MI:SS`,
			"NLS_TIMESTAMP_FORMAT":    `YYYY-MM-DD"T"HH24:MI:SS.FF6`,
			"NLS_TIMESTAMP_TZ_FORMAT": `YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM`,
			"NLS_TIME_FORMAT":         `HH24:MI:SS.FF6`,
			"NLS_TIME_TZ_FORMAT":      `HH24:MI:SS.FF6TZH:TZM`,
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
			if d.namingStrategy.IdentifierMaxLength == 0 {
				d.namingStrategy.capIdentifierMaxLength = 128
			}
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

	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return
}

func (d Dialector) stableDbNameFields(m *schema.Schema, cols []clause.Column) []*schema.Field {
	if m == nil {
		return nil
	}
	ordered := make([]*schema.Field, 0, len(m.Fields))
	for _, f := range m.Fields {
		if !isReturnableField(f) || !f.Readable {
			continue
		}
		if fld := m.LookUpField(f.Name); fld != nil {
			if len(cols) == 0 || !slices.ContainsFunc(cols, func(c clause.Column) bool {
				return c.Name == fld.DBName
			}) {
				ordered = append(ordered, fld)
			}
		}
	}
	return ordered
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
	return
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

var ty16Byte = reflect.TypeOf((*[16]byte)(nil)).Elem()

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
		if dbVer, _ := strconv.Atoi(strings.Split(d.DBVer, ".")[0]); dbVer >= 23 {
			booleanType = "BOOLEAN"
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
	case schema.String, "VARCHAR2":
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
				sqlType = "VARCHAR2(4000)"
			}
		}
	case schema.Time:
		sqlType = "TIMESTAMP WITH TIME ZONE"
	case schema.Bytes:
		sqlType = "BLOB"
	case "timestamp without time zone":
		sqlType = "TIMESTAMP WITH LOCAL TIME ZONE"
	case "timestamp":
		sqlType = "TIMESTAMP"
	case "date":
		sqlType = "DATE"
	default:
		sqlType = string(field.DataType)

		if strings.EqualFold(sqlType, "text") {
			if d.Config.UseClobForTextType {
				sqlType = "CLOB"
			} else {
				sqlType = "VARCHAR2(4000)"
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

func toBytesFrom16Array(val interface{}) ([]byte, error) {
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()

	// check it’s an Array of length 16
	if rt.Kind() != reflect.Array || rt.Len() != 16 {
		return nil, fmt.Errorf("expected array[16], got %s", rt)
	}
	// check element kind is uint8
	if rt.Elem().Kind() != reflect.Uint8 {
		return nil, fmt.Errorf("expected element kind uint8, got %s", rt.Elem())
	}

	// build a new slice and copy each element
	out := make([]byte, 16)
	for i := 0; i < 16; i++ {
		// rv.Index(i) gives a reflect.Value of the element
		// .Uint() returns its numeric (0–255) value
		out[i] = byte(rv.Index(i).Uint())
	}
	return out, nil
}
