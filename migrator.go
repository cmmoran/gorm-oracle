package oracle

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"

	"github.com/iancoleman/strcase"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// Migrator implement gorm migrator interface
type Migrator struct {
	migrator.Migrator
	namingStrategy *NamingStrategy
}

// AutoMigrate automatically migrate model to table structure
//
//	// Migrate and set single table comment
//	db.Set("gorm:table_comments", "User Information Table").AutoMigrate(&User{})
//
//	// Migrate and set multiple table comments
//	db.Set("gorm:table_comments", []string{"User Information Table", "Company Information Table"}).AutoMigrate(&User{}, &Company{})
func (m Migrator) AutoMigrate(dst ...interface{}) error {
	if err := m.Migrator.AutoMigrate(dst...); err != nil {
		return err
	}
	// set table comment
	if tableComments, ok := m.DB.Get("gorm:table_comments"); ok {
		for i := 0; i < len(dst); i++ {
			if i >= 1 && (i >= len(dst)) {
				break
			}
			if err := m.RunWithValue(dst[i], func(stmt *gorm.Statement) error {
				switch c := tableComments.(type) {
				case string:
					return m.setTableComment(stmt.Table, c)
				case []string:
					if i < len(c) {
						return m.setTableComment(stmt.Table, c[i])
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// FullDataTypeOf returns field's db full data type
func (m Migrator) FullDataTypeOf(field *schema.Field) (expr clause.Expr) {
	expr.SQL = m.DataTypeOf(field)

	if field.HasDefaultValue && (field.DefaultValueInterface != nil || field.DefaultValue != "") {
		if field.DefaultValueInterface != nil {
			defaultStmt := &gorm.Statement{Vars: []interface{}{field.DefaultValueInterface}}
			m.Dialector.BindVarTo(defaultStmt, defaultStmt, field.DefaultValueInterface)
			expr.SQL += " DEFAULT " + m.Dialector.Explain(defaultStmt.SQL.String(), field.DefaultValueInterface)
		} else if field.DefaultValue != "(-)" {
			expr.SQL += " DEFAULT " + field.DefaultValue
		}
	}

	if field.NotNull {
		expr.SQL += " NOT NULL"
	}

	return
}

// CurrentDatabase returns current database name
func (m Migrator) CurrentDatabase() (name string) {
	_ = m.DB.Raw(
		fmt.Sprintf(`SELECT ORA_DATABASE_NAME as "Current Database" FROM %s`, m.Dialector.(Dialector).DummyTableName()),
	).Row().Scan(&name)
	return
}

// GetTypeAliases return database type aliases
func (m Migrator) GetTypeAliases(databaseTypeName string) (types []string) {
	switch databaseTypeName {
	case "blob", "raw", "longraw", "ocibloblocator", "ocifilelocator":
		types = append(types, "blob", "raw", "longraw", "ocibloblocator", "ocifilelocator")
	case "clob", "nclob", "longvarchar", "ocicloblocator":
		types = append(types, "clob", "nclob", "longvarchar", "ocicloblocator")
	case "char", "nchar", "varchar", "varchar2", "nvarchar2":
		types = append(types, "char", "nchar", "varchar", "varchar2", "nvarchar2")
	case "number", "integer", "smallint":
		types = append(types, "number", "integer", "smallint")
	case "decimal", "numeric", "ibfloat", "ibdouble":
		types = append(types, "decimal", "numeric", "ibfloat", "ibdouble")
	case "timestampdty", "timestamp", "date":
		types = append(types, "timestampdty", "timestamp", "date")
	case "timestamptz_dty", "timestamp with time zone":
		types = append(types, "timestamptz_dty", "timestamp with time zone")
	case "timestampltz_dty", "timestampeltz", "timestamp with local time zone":
		types = append(types, "timestampltz_dty", "timestampeltz", "timestamp with local time zone")
	default:
		return
	}
	return
}

// CreateTable create table in database for values
func (m Migrator) CreateTable(values ...interface{}) error {
	tx := m.DB.Session(&gorm.Session{})

	for _, value := range m.ReorderModels(values, false) {
		var pendingIdxes []*schema.Index

		if err := m.RunWithValue(value, func(stmt *gorm.Statement) (err error) {
			if stmt.Schema == nil {
				return fmt.Errorf("oracle: failed to get schema")
			}
			ns := getNS(m.DB, m.Dialector)

			sqlBuf := "CREATE TABLE ? ("
			binds := []interface{}{m.CurrentTable(stmt)}
			hasPrimaryKeyInDataType := false

			// columns
			for _, dbName := range stmt.Schema.DBNames {
				f := stmt.Schema.FieldsByDBName[dbName]
				if f.IgnoreMigration {
					continue
				}
				sqlBuf += "? ?"
				if strings.Contains(strings.ToUpper(m.DataTypeOf(f)), "PRIMARY KEY") {
					hasPrimaryKeyInDataType = true
				}
				binds = append(binds, clause.Column{Name: dbName}, m.DB.Migrator().FullDataTypeOf(f))
				sqlBuf += ","
			}

			// PK constraint (named, Oracle-safe)
			if !hasPrimaryKeyInDataType && len(stmt.Schema.PrimaryFields) > 0 {
				sqlBuf += "CONSTRAINT ? PRIMARY KEY ("
				var pkCols []interface{}
				var pkColNames []string
				for i, pf := range stmt.Schema.PrimaryFields {
					if i > 0 {
						sqlBuf += ","
					}
					sqlBuf += "?"
					pkCols = append(pkCols, clause.Column{Name: pf.DBName})
					pkColNames = append(pkColNames, pf.DBName)
				}
				sqlBuf += "),"
				pkName := ns.UniqueName(stmt.Table, strings.Join(pkColNames, "_"))
				binds = append(binds, clause.Column{Name: pkName, Raw: true})
				binds = append(binds, pkCols...)
			}

			// FKs / CHECK / UNIQUE (inline constraints, not indexes)
			if !m.DB.DisableForeignKeyConstraintWhenMigrating && !m.DB.IgnoreRelationshipsWhenMigrating {
				for _, rel := range stmt.Schema.Relationships.Relations {
					if rel.Field.IgnoreMigration {
						continue
					}
					if c := rel.ParseConstraint(); c != nil && c.Schema == stmt.Schema {
						// Oracle: no ON UPDATE
						c.OnUpdate = ""                  // primary fix
						sqlFrag, vars := c.Build()       // build SQL + binds
						sqlFrag = stripOnUpdate(sqlFrag) // guard: remove any residual "ON UPDATE ..."
						sqlBuf += sqlFrag + ","
						binds = append(binds, vars...)
					}
				}
			}
			for _, uni := range stmt.Schema.ParseUniqueConstraints() {
				// single-column unique constraint
				sqlBuf += "CONSTRAINT ? UNIQUE (?),"
				binds = append(
					binds,
					clause.Column{Name: uni.Name, Raw: true},
					clause.Column{Name: uni.Field.DBName},
				)
			}
			for _, chk := range stmt.Schema.ParseCheckConstraints() {
				sqlBuf += "CONSTRAINT ? CHECK (?),"
				binds = append(binds, clause.Column{Name: chk.Name, Raw: true}, clause.Expr{SQL: chk.Constraint})
			}

			// collect indexes for post-create CreateIndex
			for _, idx := range stmt.Schema.ParseIndexes() {
				pendingIdxes = append(pendingIdxes, idx)
			}

			sqlBuf = strings.TrimSuffix(sqlBuf, ",") + ")"

			// no MySQL-style table options

			if err = tx.Exec(sqlBuf, binds...).Error; err != nil {
				return err
			}

			return nil
		}); err != nil {
			return err
		}

		// post-create phase: create indexes
		for _, idx := range pendingIdxes {
			if !tx.Migrator().HasIndex(value, idx.Name) {
				if err := tx.Migrator().CreateIndex(value, idx.Name); err != nil {
					return err
				}
			}

		}
	}
	// set table comment
	if tableComments, ok := m.DB.Get("gorm:table_comments"); ok {
		for i := 0; i < len(values); i++ {
			if i >= 1 && (i >= len(values)) {
				break
			}
			if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
				switch c := tableComments.(type) {
				case string:
					return m.setTableComment(stmt.Table, c)
				case []string:
					if i < len(c) {
						return m.setTableComment(stmt.Table, c[i])
					}
				}
				if stmt.Schema != nil {
					for _, f := range stmt.Schema.Fields {
						if strings.TrimSpace(f.Comment) != "" && !f.IgnoreMigration {
							if err := m.setColumnComment(stmt.Table, f.DBName, f.Comment); err != nil {
								return err
							}
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

// DropTable drop table for values
//
//goland:noinspection SqlNoDataSourceInspection
func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)

	for i := len(values) - 1; i >= 0; i-- {
		val := values[i]
		if !m.HasTable(val) {
			continue
		}
		if err := m.RunWithValue(val, func(stmt *gorm.Statement) error {
			rawSql := "DROP TABLE ? CASCADE CONSTRAINTS"
			if purge, ok := m.DB.Get("oracle:purge_on_drop"); ok && purge == true {
				rawSql += " PURGE"
			}
			return m.DB.Exec(rawSql, m.CurrentTable(stmt)).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

// HasTable returns table existence using Oracle data dictionary.
// Uses dictQualifiedParts to compare OWNER/TABLE_NAME correctly for quoted vs unquoted identifiers.
func (m Migrator) HasTable(value interface{}) bool {
	ns := getNS(m.DB, m.Dialector)

	var exists int
	err := m.RunWithValue(value, func(s *gorm.Statement) error {
		owner, object, hasOwner := ns.dictQualifiedParts(s.Table)
		if hasOwner {
			return m.DB.Raw(
				`SELECT 1 FROM ALL_TABLES WHERE OWNER = :owner AND TABLE_NAME = :obj AND ROWNUM = 1`,
				sql.Named("owner", owner), sql.Named("obj", object),
			).Scan(&exists).Error
		}
		return m.DB.Raw(
			`SELECT 1 FROM USER_TABLES WHERE TABLE_NAME = :obj AND ROWNUM = 1`,
			sql.Named("obj", object),
		).Scan(&exists).Error
	})
	return err == nil && exists == 1
}

// ColumnTypes via USER/ALL_TAB_COLUMNS (no driver metadata).
func (m Migrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
	ns := getNS(m.DB, m.Dialector)
	var out []gorm.ColumnType

	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		owner, tab, hasOwner := ns.dictQualifiedParts(stmt.Table)

		type row struct {
			Name        string         `gorm:"column:column_name"`
			DataType    string         `gorm:"column:data_type"`
			DataLength  sql.NullInt64  `gorm:"column:data_length"`
			Precision   sql.NullInt64  `gorm:"column:data_precision"`
			Scale       sql.NullInt64  `gorm:"column:data_scale"`
			Nullable    string         `gorm:"column:nullable"`     // 'Y' or 'N'
			DataDefault sql.NullString `gorm:"column:data_default"` // raw default text
		}
		var rows []row

		var q string
		var args []interface{}
		if hasOwner {
			q = `
				SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT
				  FROM ALL_TAB_COLUMNS
				 WHERE OWNER = :owner AND TABLE_NAME = :tab
				 ORDER BY COLUMN_ID`
			args = []interface{}{sql.Named("owner", owner), sql.Named("tab", tab)}
		} else {
			q = `
				SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT
				  FROM USER_TAB_COLUMNS
				 WHERE TABLE_NAME = :tab
				 ORDER BY COLUMN_ID`
			args = []interface{}{sql.Named("tab", tab)}
		}

		if err := m.DB.Raw(q, args...).Scan(&rows).Error; err != nil {
			return err
		}

		for _, r := range rows {
			ct := migrator.ColumnType{}

			// Required by GORM for existence checks:
			ct.NameValue = sql.NullString{String: r.Name, Valid: true}
			ct.DataTypeValue = sql.NullString{String: r.DataType, Valid: true}

			// Optional metadata (only set when present):
			if r.Nullable != "" {
				ct.NullableValue = sql.NullBool{Bool: strings.EqualFold(r.Nullable, "Y"), Valid: true}
			}
			if r.DataLength.Valid {
				ct.LengthValue = r.DataLength
			}
			if r.Precision.Valid {
				ct.DecimalSizeValue = r.Precision
			}
			if r.Scale.Valid {
				ct.ScaleValue = r.Scale
			}
			if r.DataDefault.Valid {
				ct.DefaultValueValue = sql.NullString{String: strings.TrimSpace(r.DataDefault.String), Valid: true}
			}

			out = append(out, ct)
		}
		return nil
	})

	return out, err
}

// RenameTable rename table from oldName to newName
func (m Migrator) RenameTable(oldName, newName interface{}) (err error) {
	resolveTable := func(name interface{}) (result string, err error) {
		if v, ok := name.(string); ok {
			result = v
		} else {
			stmt := &gorm.Statement{DB: m.DB}
			if err = stmt.Parse(name); err == nil {
				result = stmt.Table
			}
		}
		return
	}

	var oldTable, newTable string

	if oldTable, err = resolveTable(oldName); err != nil {
		return
	}

	if newTable, err = resolveTable(newName); err != nil {
		return
	}

	if !m.HasTable(oldTable) {
		return
	}

	return m.DB.Exec(
		"ALTER TABLE ? RENAME TO ?",
		clause.Table{Name: oldTable},
		clause.Table{Name: newTable},
	).Error
}

// GetTables returns tables under the current user database
func (m Migrator) GetTables() (tableList []string, err error) {
	err = m.DB.Raw(`SELECT TABLE_NAME FROM USER_TABLES
		WHERE TABLESPACE_NAME IS NOT NULL AND TABLESPACE_NAME <> 'SYSAUX'
			AND TABLE_NAME NOT LIKE 'AQ$%' AND TABLE_NAME NOT LIKE 'MVIEW$%' AND TABLE_NAME NOT LIKE 'ROLLING$%'
			AND TABLE_NAME NOT IN ('HELP', 'SQLPLUS_PRODUCT_PROFILE', 'LOGSTDBY$PARAMETERS', 'LOGMNRGGC_GTCS', 'LOGMNRGGC_GTLO', 'LOGMNR_PARAMETER$', 'LOGMNR_SESSION$', 'SCHEDULER_JOB_ARGS_TBL', 'SCHEDULER_PROGRAM_ARGS_TBL')
		`).Scan(&tableList).Error
	return
}

// AddColumn adds a column using Oracle syntax:
//
// ALTER TABLE <t> ADD (<col …>)
// Then (optionally) enforce NOT NULL via a separate MODIFY to avoid data population issues.
func (m Migrator) AddColumn(value interface{}, field string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema == nil {
			return gorm.ErrModelValueRequired
		}

		sf := stmt.Schema.LookUpField(field)
		if sf == nil {
			return fmt.Errorf("oracle: AddColumn: field %q not found", field)
		}

		// ---- guard: don't attempt to add an existing column ----
		if m.HasColumn(value, sf.DBName) {
			return nil
		}

		// Build definition for ADD: include identity, skip nullability here.
		def := m.buildColumnFragment(sf, nil, columnFragOpts{
			forAlter:        false,
			nullability:     NullNoop,
			includeIdentity: true,
		})

		var add strings.Builder
		add.WriteString("ALTER TABLE ")
		m.DB.Dialector.QuoteTo(&add, stmt.Table)
		add.WriteString(" ADD (")
		add.WriteString(def)
		add.WriteByte(')')

		if err := m.DB.Exec(add.String()).Error; err != nil {
			return err
		}

		// Enforce NOT NULL separately if required.
		if sf.NotNull {
			var mod strings.Builder
			mod.WriteString("ALTER TABLE ")
			m.DB.Dialector.QuoteTo(&mod, stmt.Table)
			mod.WriteString(" MODIFY (")
			// For MODIFY enforce nullability and no identity clause
			//mod.WriteString(m.buildColumnFragment(sf, nil, true /*forAlter*/, true /*includeNullability*/, false /*includeIdentity*/))
			mod.WriteString(m.buildColumnFragment(sf, nil, columnFragOpts{
				forAlter:        true,
				nullability:     NullSetNotNull,
				includeIdentity: false,
			}))
			mod.WriteByte(')')
			if err := m.DB.Exec(mod.String()).Error; err != nil {
				return err
			}
		}

		// Column comment (optional)
		if strings.TrimSpace(sf.Comment) != "" {
			if err := m.setColumnComment(stmt.Table, sf.DBName, sf.Comment); err != nil {
				return err
			}
		}

		return nil
	})
}

type nullAction int

const (
	NullNoop nullAction = iota
	NullSetNull
	NullSetNotNull
)

type columnFragOpts struct {
	forAlter        bool
	nullability     nullAction // only emit when not NullNoop
	includeIdentity bool
	dropDefault     bool // when true and dictDefault non-empty & model has no default -> emit "DEFAULT NULL"
}

// buildColumnFragment emits:
//
//	<quoted-col> <datatype> [DEFAULT …] [NULL|NOT NULL] [GENERATED BY DEFAULT AS IDENTITY]
//
// Options:
//   - forAlter: when true and there is no desired default but dictionary has one, append "DEFAULT NULL" to drop it
//   - includeNullability: when true, append "NULL"/"NOT NULL" based on sf.NotNull
//   - includeIdentity: when true and sf.AutoIncrement, append identity clause (Oracle 12c+)
func (m Migrator) buildColumnFragment(
	sf *schema.Field,
	dictDefault *sql.NullString, // may be nil for ADD
	opts columnFragOpts,
) string {
	var frag strings.Builder

	// <quoted-col>
	m.DB.Dialector.QuoteTo(&frag, sf.DBName)
	frag.WriteByte(' ')

	// <datatype>
	dt := m.DataTypeOf(sf) // IMPORTANT: DataTypeOf, not FullDataTypeOf
	udt := strings.ToUpper(dt)
	frag.WriteString(dt)

	// [DEFAULT …]
	switch {
	case sf.DefaultValueInterface != nil:
		if s, ok := sf.DefaultValueInterface.(string); ok {
			frag.WriteString(" DEFAULT '")
			for _, r := range s {
				if r == '\'' {
					frag.WriteString("''")
				} else {
					frag.WriteRune(r)
				}
			}
			frag.WriteByte('\'')
		} else {
			frag.WriteString(" DEFAULT ")
			frag.WriteString(toSQLLiteral(sf.DefaultValueInterface))
		}
	case sf.HasDefaultValue && strings.TrimSpace(sf.DefaultValue) != "" && sf.DefaultValue != "(-)":
		frag.WriteString(" DEFAULT ")
		frag.WriteString(sf.DefaultValue)
	default:
		// only in ALTER: drop an existing default if model has no default
		if opts.forAlter && opts.dropDefault && dictDefault != nil &&
			dictDefault.Valid && strings.TrimSpace(dictDefault.String) != "" {
			frag.WriteString(" DEFAULT NULL")
		}
	}

	// [NULL|NOT NULL] only when requested
	switch opts.nullability {
	case NullSetNull:
		frag.WriteString(" NULL")
	case NullSetNotNull:
		frag.WriteString(" NOT NULL")
	}

	// [GENERATED BY DEFAULT AS IDENTITY] only if not already present
	if opts.includeIdentity && sf.AutoIncrement &&
		!(strings.Contains(udt, "GENERATED") && strings.Contains(udt, "AS IDENTITY")) {
		frag.WriteString(" GENERATED BY DEFAULT AS IDENTITY")
	}

	return frag.String()
}

// DropColumn ALTER TABLE <table> DROP COLUMN <col>
func (m Migrator) DropColumn(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema == nil {
			return gorm.ErrModelValueRequired
		}
		f := stmt.Schema.LookUpField(name)
		if f == nil {
			return fmt.Errorf("oracle: DropColumn: field %q not found", name)
		}

		var rawSql strings.Builder
		rawSql.WriteString("ALTER TABLE ")
		m.DB.Dialector.QuoteTo(&rawSql, stmt.Table)
		rawSql.WriteString(" DROP COLUMN ")
		m.DB.Dialector.QuoteTo(&rawSql, f.DBName)

		return m.DB.Exec(rawSql.String()).Error
	})
}

// AlterColumn
//
// ALTER TABLE <t> MODIFY (<col …>)
// Identity add/drop is done as a separate MODIFY.
func (m Migrator) AlterColumn(value interface{}, field string) error {
	if !m.HasColumn(value, field) {
		return nil
	}

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema == nil {
			return gorm.ErrModelValueRequired
		}
		sf := stmt.Schema.LookUpField(field)
		if sf == nil {
			return fmt.Errorf("oracle: AlterColumn: field %q not found", field)
		}

		ns := getNS(m.DB, m.Dialector)
		owner, tab, hasOwner := ns.dictQualifiedParts(stmt.Table)
		col := ns.dictCasePart(sf.DBName)

		var curDefault sql.NullString
		var curNullable string
		var hasIdentity int

		if hasOwner {
			_ = m.DB.Raw(`
                SELECT c.DATA_DEFAULT, c.NULLABLE
                  FROM ALL_TAB_COLUMNS c
                 WHERE c.OWNER = :owner AND c.TABLE_NAME = :tab AND c.COLUMN_NAME = :col`,
				sql.Named("owner", owner), sql.Named("tab", tab), sql.Named("col", col),
			).Row().Scan(&curDefault, &curNullable)

			_ = m.DB.Raw(`
                SELECT 1 FROM ALL_TAB_IDENTITY_COLS
                 WHERE OWNER = :owner AND TABLE_NAME = :tab AND COLUMN_NAME = :col AND ROWNUM = 1`,
				sql.Named("owner", owner), sql.Named("tab", tab), sql.Named("col", col),
			).Row().Scan(&hasIdentity)
		} else {
			_ = m.DB.Raw(`
                SELECT c.DATA_DEFAULT, c.NULLABLE
                  FROM USER_TAB_COLUMNS c
                 WHERE c.TABLE_NAME = :tab AND c.COLUMN_NAME = :col`,
				sql.Named("tab", tab), sql.Named("col", col),
			).Row().Scan(&curDefault, &curNullable)

			_ = m.DB.Raw(`
                SELECT 1 FROM USER_TAB_IDENTITY_COLS
                 WHERE TABLE_NAME = :tab AND COLUMN_NAME = :col AND ROWNUM = 1`,
				sql.Named("tab", tab), sql.Named("col", col),
			).Row().Scan(&hasIdentity)
		}

		// decide nullability delta
		var na nullAction = NullNoop
		isCurNullable := strings.EqualFold(curNullable, "Y")
		switch {
		case sf.NotNull && isCurNullable:
			na = NullSetNotNull
		case !sf.NotNull && !isCurNullable:
			na = NullSetNull
		}

		// identity columns: avoid default/nullability changes in the main MODIFY
		if hasIdentity == 1 {
			na = NullNoop
		}

		// dropDefault only if model has no default
		wantModelDefault := (sf.DefaultValueInterface != nil) ||
			(sf.HasDefaultValue && strings.TrimSpace(sf.DefaultValue) != "")
		dropDef := !wantModelDefault && hasIdentity != 1

		// desired database type text for this field
		targetDT := m.DataTypeOf(sf)

		// If target is LOB/LONG, use rewrite path instead of MODIFY
		if targetIsLOB(targetDT) {
			return m.rewriteColumnToLOB(stmt, sf, targetDT) // see below
		}

		frag := m.buildColumnFragment(sf, &curDefault, columnFragOpts{
			forAlter:        true,
			nullability:     na,
			includeIdentity: false, // identity handled separately below
			dropDefault:     dropDef,
		})

		var alter strings.Builder
		alter.WriteString("ALTER TABLE ")
		m.DB.Dialector.QuoteTo(&alter, stmt.Table)
		alter.WriteString(" MODIFY (")
		alter.WriteString(frag)
		alter.WriteByte(')')
		if err := m.DB.Exec(alter.String()).Error; err != nil {
			return err
		}

		// identity add/drop separate
		if sf.AutoIncrement && hasIdentity != 1 {
			// ADD IDENTITY
			var ai strings.Builder
			ai.WriteString("ALTER TABLE ")
			m.DB.Dialector.QuoteTo(&ai, stmt.Table)
			ai.WriteString(" MODIFY (")
			m.DB.Dialector.QuoteTo(&ai, sf.DBName)
			ai.WriteString(" GENERATED BY DEFAULT AS IDENTITY)")
			if err := m.DB.Exec(ai.String()).Error; err != nil {
				return err
			}
		} else if !sf.AutoIncrement && hasIdentity == 1 {
			// DROP IDENTITY
			var di strings.Builder
			di.WriteString("ALTER TABLE ")
			m.DB.Dialector.QuoteTo(&di, stmt.Table)
			di.WriteString(" MODIFY (")
			m.DB.Dialector.QuoteTo(&di, sf.DBName)
			di.WriteString(" DROP IDENTITY)")
			if err := m.DB.Exec(di.String()).Error; err != nil {
				return err
			}
		}

		// comment sync
		if strings.TrimSpace(sf.Comment) != "" {
			if err := m.setColumnComment(stmt.Table, sf.DBName, sf.Comment); err != nil {
				return err
			}
		}
		return nil
	})
}

// rewriteColumnToLOB performs: ADD temp -> UPDATE copy/cast -> DROP old -> RENAME temp -> reapply extras.
// Works for CLOB/BLOB/NCLOB targets. Reserved words (e.g. "DESC") are quoted via QuoteTo.
func (m Migrator) rewriteColumnToLOB(stmt *gorm.Statement, sf *schema.Field, targetDT string) error {
	// 1) create a unique temp column name (unquoted identifier namespace)
	tmp := fmt.Sprintf("%s_TMP_%08X", strcase.ToScreamingSnake(sf.DBName), fnv32(sf.DBName+targetDT))

	// 2) ADD temp column (no default/nullability here)
	{
		var b strings.Builder
		b.WriteString("ALTER TABLE ")
		m.DB.Dialector.QuoteTo(&b, stmt.Table)
		b.WriteString(" ADD (")
		m.DB.Dialector.QuoteTo(&b, tmp)
		b.WriteByte(' ')
		b.WriteString(targetDT)
		b.WriteByte(')')
		if err := m.DB.Exec(b.String()).Error; err != nil {
			return err
		}
	}

	// 3) UPDATE temp from old col with an appropriate cast
	copyExpr, err := lobCopyExpr(m.DB, sf.DBName, targetDT)
	if err != nil {
		return err
	}

	{
		var b strings.Builder
		b.WriteString("UPDATE ")
		m.DB.Dialector.QuoteTo(&b, stmt.Table)
		b.WriteString(" SET ")
		m.DB.Dialector.QuoteTo(&b, tmp)
		b.WriteString(" = ")
		b.WriteString(copyExpr)
		if err := m.DB.Exec(b.String()).Error; err != nil {
			return err
		}
	}

	// 4) DROP old column
	{
		var b strings.Builder
		b.WriteString("ALTER TABLE ")
		m.DB.Dialector.QuoteTo(&b, stmt.Table)
		b.WriteString(" DROP COLUMN ")
		m.DB.Dialector.QuoteTo(&b, sf.DBName)
		if err := m.DB.Exec(b.String()).Error; err != nil {
			return err
		}
	}

	// 5) RENAME temp -> original
	{
		var b strings.Builder
		b.WriteString("ALTER TABLE ")
		m.DB.Dialector.QuoteTo(&b, stmt.Table)
		b.WriteString(" RENAME COLUMN ")
		m.DB.Dialector.QuoteTo(&b, tmp)
		b.WriteString(" TO ")
		m.DB.Dialector.QuoteTo(&b, sf.DBName)
		if err := m.DB.Exec(b.String()).Error; err != nil {
			return err
		}
	}

	// 6) Re-apply NOT NULL (if needed)
	if sf.NotNull {
		var b strings.Builder
		b.WriteString("ALTER TABLE ")
		m.DB.Dialector.QuoteTo(&b, stmt.Table)
		b.WriteString(" MODIFY (")
		m.DB.Dialector.QuoteTo(&b, sf.DBName)
		b.WriteString(" ")
		b.WriteString(targetDT)
		b.WriteString(" NOT NULL)")
		if err := m.DB.Exec(b.String()).Error; err != nil {
			return err
		}
	}

	// 7) Re-apply comment (if any)
	if strings.TrimSpace(sf.Comment) != "" {
		if err := m.setColumnComment(stmt.Table, sf.DBName, sf.Comment); err != nil {
			return err
		}
	}

	return nil
}

// Build the expression to copy old -> new when converting to a LOB.
func lobCopyExpr(db *gorm.DB, srcCol string, targetDT string) (string, error) {
	u := strings.ToUpper(targetDT)

	// quoted source column name
	var src strings.Builder
	db.Dialector.QuoteTo(&src, srcCol)

	switch {
	case strings.Contains(u, "CLOB") || strings.Contains(u, "NCLOB"):
		// from VARCHAR2/CHAR -> CLOB: TO_CLOB(src)
		// from NUMBER/DATE -> TO_CLOB(TO_CHAR(src)) — basic fallback
		return "TO_CLOB(" + src.String() + ")", nil

	case strings.Contains(u, "BLOB"):
		// From RAW/VARCHAR2 -> BLOB: cast to RAW then to BLOB; safest generic is empty for non-RAW sources.
		// If your models only go VARCHAR2->BLOB, you can use UTL_RAW.CAST_TO_RAW, then TO_BLOB is not a function.
		// Prefer: HEXTORAW for hex text, or UTL_RAW.CAST_TO_RAW for text bytes.
		// Minimal fallback for VARCHAR2:
		return "UTL_RAW.CAST_TO_RAW(" + src.String() + ")", nil
	}
	return "", fmt.Errorf("unsupported LOB target: %s", targetDT)
}

func fnv32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// HasColumn returns whether a column exists on the target table.
func (m Migrator) HasColumn(value interface{}, field string) bool {
	ns := getNS(m.DB, m.Dialector)

	var exists int
	err := m.RunWithValue(value, func(s *gorm.Statement) error {
		owner, table, hasOwner := ns.dictQualifiedParts(s.Table)
		col := ns.dictCasePart(field)

		if hasOwner {
			return m.DB.Raw(
				`SELECT 1
				   FROM ALL_TAB_COLUMNS
				  WHERE OWNER = :owner
				    AND TABLE_NAME = :tab
				    AND COLUMN_NAME = :col
				    AND ROWNUM = 1`,
				sql.Named("owner", owner),
				sql.Named("tab", table),
				sql.Named("col", col),
			).Scan(&exists).Error
		}

		return m.DB.Raw(
			`SELECT 1
			   FROM USER_TAB_COLUMNS
			  WHERE TABLE_NAME = :tab
			    AND COLUMN_NAME = :col
			    AND ROWNUM = 1`,
			sql.Named("tab", table),
			sql.Named("col", col),
		).Scan(&exists).Error
	})

	return err == nil && exists == 1
}

// MigrateColumn Oracle-specific.
// 1) ALTER via your AlterColumn (MODIFY ...).
// 2) Sync COMMENT ON COLUMN if model comment differs.
func (m Migrator) MigrateColumn(value interface{}, field *schema.Field, _ gorm.ColumnType) error {
	// 1) ALTER column to desired definition (Oracle MODIFY ... + identity handling)
	if err := m.AlterColumn(value, field.DBName); err != nil {
		return err
	}

	// 2) Comment sync (dictionary-aware)
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		ns := getNS(m.DB, m.Dialector)

		owner, table, hasOwner := ns.dictQualifiedParts(stmt.Table)
		col := ns.dictCasePart(field.DBName)

		var description sql.NullString
		var err error

		if hasOwner {
			err = m.DB.Raw(
				`SELECT COMMENTS
				   FROM ALL_COL_COMMENTS
				  WHERE OWNER = :owner AND TABLE_NAME = :tab AND COLUMN_NAME = :col AND ROWNUM = 1`,
				sql.Named("owner", owner),
				sql.Named("tab", table),
				sql.Named("col", col),
			).Scan(&description).Error
		} else {
			err = m.DB.Raw(
				`SELECT COMMENTS
				   FROM USER_COL_COMMENTS
				  WHERE TABLE_NAME = :tab AND COLUMN_NAME = :col AND ROWNUM = 1`,
				sql.Named("tab", table),
				sql.Named("col", col),
			).Scan(&description).Error
		}
		if err != nil {
			return err
		}

		comment := strings.TrimSpace(field.Comment)
		existing := strings.TrimSpace(description.String)

		if comment != "" && comment != existing {
			// COMMENT ON COLUMN <table>.<column> IS '<comment>'
			return m.setColumnComment(stmt.Table, field.DBName, comment)
		}
		return nil
	})
}

// AlterDataTypeOf builds "<datatype> [DEFAULT ...] [NOT NULL]" for Oracle.
// It is used by generic migrator code paths; AlterColumn/AddColumn should still call their own builders.
func (m Migrator) AlterDataTypeOf(stmt *gorm.Statement, field *schema.Field) (expr clause.Expr) {
	// Base datatype (dialector-specific SQL)
	expr.SQL = m.FullDataTypeOf(field).SQL

	// Lookup current NULLABLE from dictionary (to decide if we need NOT NULL)
	ns := getNS(m.DB, m.Dialector)
	owner, tab, hasOwner := ns.dictQualifiedParts(stmt.Table)
	col := ns.dictCasePart(field.DBName)

	var nullable string
	if hasOwner {
		_ = m.DB.Raw(
			`SELECT NULLABLE
			   FROM ALL_TAB_COLUMNS
			  WHERE OWNER = :owner AND TABLE_NAME = :tab AND COLUMN_NAME = :col`,
			sql.Named("owner", owner), sql.Named("tab", tab), sql.Named("col", col),
		).Row().Scan(&nullable)
	} else {
		_ = m.DB.Raw(
			`SELECT NULLABLE
			   FROM USER_TAB_COLUMNS
			  WHERE TABLE_NAME = :tab AND COLUMN_NAME = :col`,
			sql.Named("tab", tab), sql.Named("col", col),
		).Row().Scan(&nullable)
	}

	// DEFAULT (literal vs expression)
	switch {
	case field.DefaultValueInterface != nil:
		if s, ok := field.DefaultValueInterface.(string); ok {
			// string literal -> quote/escape
			var b strings.Builder
			b.WriteString(" DEFAULT '")
			for _, r := range s {
				if r == '\'' {
					b.WriteString("''")
				} else {
					b.WriteRune(r)
				}
			}
			b.WriteByte('\'')
			expr.SQL += b.String()
		} else {
			expr.SQL += " DEFAULT " + toSQLLiteral(field.DefaultValueInterface)
		}
	case field.HasDefaultValue && field.DefaultValue != "" && field.DefaultValue != "(-)":
		// expression (e.g., SYSDATE)
		expr.SQL += " DEFAULT " + field.DefaultValue
	}

	// NOT NULL (only add when current is nullable)
	if field.NotNull && strings.EqualFold(nullable, "Y") {
		expr.SQL += " NOT NULL"
	}
	// Column-level UNIQUE is allowed by Oracle (optional)
	if field.Unique {
		expr.SQL += " UNIQUE"
	}

	return
}

// CreateConstraint ensure FK names follow oracle.NamingStrategy (genToken),
// and strip unsupported ON UPDATE clauses.
func (m Migrator) CreateConstraint(value interface{}, name string) error {
	ns := getNS(m.DB, m.Dialector)

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		for _, rel := range stmt.Schema.Relationships.Relations {
			c := rel.ParseConstraint()
			if c == nil {
				continue
			}
			// GORM will call CreateConstraint(model, <constraintName>) with the name from tags or auto.
			// Only handle when the requested name matches, or when caller passes empty (create all).
			if name != "" && c.Name != name {
				continue
			}

			// 1) Build canonical FK name via genToken if missing or needs normalization.
			//    (GORM’s auto name is dialect-agnostic; we replace it with our Oracle-safe token.)
			if len(c.ForeignKeys) > 0 && c.References != nil {
				cols := make([]string, 0, len(c.ForeignKeys))
				for _, fk := range c.ForeignKeys {
					cols = append(cols, fk.DBName)
				}
				// FK_<table>_<col1_col2>
				c.Name = ns.RelationshipFKName(*rel)
			} else {
				// Non-FK constraints shouldn’t land here in practice; but if they do,
				// normalize the provided name to our case/quoting.
				n, _ := ns.normalizePart(c.Name)
				c.Name = n
			}

			// 2) Oracle: drop ON UPDATE (unsupported)
			c.OnUpdate = ""
			sqlFrag, vars := c.Build()
			sqlFrag = stripOnUpdate(sqlFrag)

			// 3) Execute
			return m.DB.Exec(sqlFrag, vars...).Error
		}
		return nil
	})
}

// DropConstraint ALTER TABLE <table> DROP CONSTRAINT <name>
func (m Migrator) DropConstraint(value interface{}, name string) error {
	ns := getNS(m.DB, m.Dialector)
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		dictName := ns.dictCasePart(name)

		return m.DB.Exec(
			"ALTER TABLE ? DROP CONSTRAINT ?",
			m.CurrentTable(stmt),
			clause.Column{Name: dictName, Raw: true},
		).Error
	})
}

// HasConstraint USER_CONSTRAINTS / ALL_CONSTRAINTS with dictionary casing
func (m Migrator) HasConstraint(value interface{}, name string) bool {
	ns := getNS(m.DB, m.Dialector)

	var exists int
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		owner, tab, hasOwner := ns.dictQualifiedParts(stmt.Table)
		cname := ns.dictCasePart(name)

		if hasOwner {
			err := m.DB.Raw(
				`SELECT 1
				   FROM ALL_CONSTRAINTS
				  WHERE OWNER = :owner
				    AND TABLE_NAME = :tab
				    AND CONSTRAINT_NAME = :c
				    AND ROWNUM = 1`,
				sql.Named("owner", owner),
				sql.Named("tab", tab),
				sql.Named("c", cname),
			).Scan(&exists).Error
			if err != nil {
				m.DB.Logger.Error(m.DB.Statement.Context, "HasConstraint failed owner:%s table:%s name:%s err:%v exists:%d", owner, tab, cname, err, exists)
			}
			return err
		}
		err := m.DB.Raw(
			`SELECT 1
			   FROM USER_CONSTRAINTS
			  WHERE TABLE_NAME = :tab
			    AND CONSTRAINT_NAME = :c
			    AND ROWNUM = 1`,
			sql.Named("tab", tab),
			sql.Named("c", cname),
		).Scan(&exists).Error
		if err != nil {
			m.DB.Logger.Error(m.DB.Statement.Context, "HasConstraint failed owner:%s table:%s name:%s err:%v exists:%d", owner, tab, cname, err, exists)
		}
		return err
	})
	return err == nil && exists == 1
}

func (m Migrator) CreateIndex(value interface{}, name string) error {
	ns := getNS(m.DB, m.Dialector)
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			domainCfg, err := parseOracleDomainIndexConfig(idx)
			if err != nil {
				return err
			}
			if err := validateOracleDomainIndexConfig(idx, domainCfg); err != nil {
				return err
			}

			if len(idx.Where) == 0 {
				opts := m.DB.Migrator().(migrator.BuildIndexOptionsInterface).BuildIndexOptions(idx.Fields, stmt)
				values := []interface{}{clause.Column{Name: ns.dictCasePart(idx.Name), Raw: true}, m.CurrentTable(stmt), opts}

				createIndexSQL := buildCreateIndexSQL(idx, domainCfg)

				return m.DB.Exec(createIndexSQL, values...).Error
			}
			if domainCfg.IndexType != "" {
				return fmt.Errorf("oracle: index %q cannot combine WHERE with oracle_indextype", idx.Name)
			}
			// Need to create the SQL for a `CREATE INDEX ? ON (CASE WHEN %s THEN %s END)` taking into account
			// the fields and the "Where" clause
			// -------------------------
			// PARTIAL INDEX WORKAROUND
			// -------------------------
			// ---- partial-index workaround for Oracle ----
			// 1) Build a CASE-wrapped expression for each indexed field
			exprs := make([]string, len(idx.Fields))
			for i, f := range idx.Fields {
				// f.DBName is just the plain column name (string)
				colName := ns.ColumnName("", f.DBName)
				exprs[i] = fmt.Sprintf(
					"CASE WHEN (%s) THEN %s END",
					idx.Where,
					colName,
				)
			}

			create := "CREATE "
			if idx.Class != "" {
				create = fmt.Sprintf("%s %s ", create, idx.Class)
			}
			using := ""
			if idx.Type != "" {
				using = fmt.Sprintf(" USING %s ", idx.Type)
			}
			comment := ""
			if idx.Comment != "" {
				comment = fmt.Sprintf(" COMMENT '%s'", idx.Comment)
			}
			opt := ""
			if idx.Option != "" {
				opt = fmt.Sprintf(" %s", idx.Option)
			}

			idxName := ns.dictCasePart(idx.Name)
			stmtTable := m.namingStrategy.dictCasePart(stmt.Table)
			str := fmt.Sprintf(`%sINDEX %s ON %s (%s) %s%s%s`, create, idxName, stmtTable, strings.Join(exprs, ","), using, comment, opt)

			return m.DB.Exec(str).Error

		}
		return nil
	})
}

type oracleDomainIndexConfig struct {
	IndexType  string
	Parameters string
}

func buildCreateIndexSQL(idx *schema.Index, domainCfg oracleDomainIndexConfig) string {
	createIndexSQL := "CREATE "
	if idx.Class != "" {
		createIndexSQL += idx.Class + " "
	}
	createIndexSQL += "INDEX ? ON ? ?"

	if domainCfg.IndexType != "" {
		createIndexSQL += " INDEXTYPE IS " + domainCfg.IndexType
		if domainCfg.Parameters != "" {
			createIndexSQL += " PARAMETERS (" + domainCfg.Parameters + ")"
		}
	} else if idx.Type != "" {
		createIndexSQL += " USING " + idx.Type
	}

	if idx.Comment != "" {
		createIndexSQL += fmt.Sprintf(" COMMENT '%s'", idx.Comment)
	}

	if idx.Option != "" {
		createIndexSQL += " " + idx.Option
	}

	return createIndexSQL
}

func validateOracleDomainIndexConfig(idx *schema.Index, domainCfg oracleDomainIndexConfig) error {
	if domainCfg.IndexType == "" {
		if domainCfg.Parameters != "" {
			return fmt.Errorf("oracle: index %q has oracle_parameters but missing oracle_indextype", idx.Name)
		}
		return nil
	}

	if domainCfg.Parameters != "" && !isSingleQuoted(domainCfg.Parameters) {
		return fmt.Errorf("oracle: index %q oracle_parameters must be single-quoted, got %q (example: 'SYNC (ON COMMIT)')", idx.Name, domainCfg.Parameters)
	}

	if strings.EqualFold(strings.TrimSpace(idx.Class), "UNIQUE") {
		return fmt.Errorf("oracle: index %q cannot be UNIQUE when oracle_indextype is set", idx.Name)
	}

	if strings.TrimSpace(idx.Type) != "" {
		return fmt.Errorf("oracle: index %q cannot use both type=%q and oracle_indextype=%q", idx.Name, idx.Type, domainCfg.IndexType)
	}

	return nil
}

func parseOracleDomainIndexConfig(idx *schema.Index) (oracleDomainIndexConfig, error) {
	cfg := oracleDomainIndexConfig{}
	if idx == nil {
		return cfg, nil
	}

	for _, indexField := range idx.Fields {
		if indexField.Field == nil {
			continue
		}
		field := indexField.Field

		if err := mergeOracleDomainIndexConfig(&cfg, field.TagSettings["ORACLE_INDEXTYPE"], field.TagSettings["ORACLE_PARAMETERS"], idx.Name, field.Name); err != nil {
			return cfg, err
		}

		tag := field.Tag.Get("gorm")
		if strings.TrimSpace(tag) == "" {
			continue
		}

		indexDeclCount := countIndexDeclarationsInTag(tag)
		for _, token := range strings.Split(tag, ";") {
			parts := strings.Split(token, ":")
			if len(parts) == 0 {
				continue
			}
			key := strings.ToUpper(strings.TrimSpace(parts[0]))
			if key != "INDEX" && key != "UNIQUEINDEX" {
				continue
			}

			raw := ""
			if len(parts) > 1 {
				raw = strings.TrimSpace(strings.Join(parts[1:], ":"))
			}
			if raw == "" {
				continue
			}

			declaredName := raw
			settingsPart := ""
			if splitAt := strings.IndexByte(raw, ','); splitAt >= 0 {
				declaredName = raw[:splitAt]
				settingsPart = raw[splitAt+1:]
			}
			declaredName = strings.TrimSpace(declaredName)

			// If a field has multiple unnamed index declarations, there isn't enough information here
			// to safely map options to this specific schema index.
			if declaredName == "" && indexDeclCount > 1 {
				continue
			}
			if declaredName != "" && declaredName != idx.Name {
				continue
			}

			settings := schema.ParseTagSetting(settingsPart, ",")
			if err := mergeOracleDomainIndexConfig(&cfg, settings["ORACLE_INDEXTYPE"], settings["ORACLE_PARAMETERS"], idx.Name, field.Name); err != nil {
				return cfg, err
			}
		}
	}

	return cfg, nil
}

func mergeOracleDomainIndexConfig(cfg *oracleDomainIndexConfig, indexType, parameters, indexName, fieldName string) error {
	indexType = strings.TrimSpace(indexType)
	parameters = strings.TrimSpace(parameters)

	if indexType != "" {
		if cfg.IndexType == "" {
			cfg.IndexType = indexType
		} else if cfg.IndexType != indexType {
			return fmt.Errorf("oracle: index %q has conflicting oracle_indextype values (%q vs %q) on field %q", indexName, cfg.IndexType, indexType, fieldName)
		}
	}

	if parameters != "" {
		if cfg.Parameters == "" {
			cfg.Parameters = parameters
		} else if cfg.Parameters != parameters {
			return fmt.Errorf("oracle: index %q has conflicting oracle_parameters values (%q vs %q) on field %q", indexName, cfg.Parameters, parameters, fieldName)
		}
	}

	return nil
}

func countIndexDeclarationsInTag(tag string) int {
	count := 0
	for _, token := range strings.Split(tag, ";") {
		parts := strings.Split(token, ":")
		if len(parts) == 0 {
			continue
		}
		key := strings.ToUpper(strings.TrimSpace(parts[0]))
		if key == "INDEX" || key == "UNIQUEINDEX" {
			count++
		}
	}
	return count
}

func isSingleQuoted(v string) bool {
	v = strings.TrimSpace(v)
	return len(v) >= 2 && strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'")
}

// BuildIndexOptions builds the per-column list for CREATE INDEX on Oracle.
// Notes:
// - Ignore Length and Collate (not applicable).
// - Keep raw expressions as-is.
// - Use NamingStrategy to render identifiers (avoids quotes unless required).
// - Allow ASC/DESC [NULLS FIRST|LAST].
func (m Migrator) BuildIndexOptions(opts []schema.IndexOption, stmt *gorm.Statement) (results []interface{}) {
	ns := getNS(m.DB, m.Dialector)

	for _, opt := range opts {
		// 1) Expression element
		if s := strings.TrimSpace(opt.Expression); s != "" {
			results = append(results, clause.Expr{SQL: s})
			continue
		}

		// 2) Identifier rendered by our naming strategy
		name, quoted := ns.normalizePart(opt.DBName)
		var b strings.Builder
		if quoted {
			// write "name" with inner quotes doubled
			b.WriteByte('"')
			for _, r := range name {
				if r == '"' {
					b.WriteString(`""`)
				} else {
					b.WriteRune(r)
				}
			}
			b.WriteByte('"')
		} else {
			b.WriteString(name)
		}

		// 3) Optional sort (Oracle-supported tokens only)
		if s := strings.ToUpper(strings.TrimSpace(opt.Sort)); s != "" {
			switch s {
			case "ASC", "DESC",
				"ASC NULLS FIRST", "ASC NULLS LAST",
				"DESC NULLS FIRST", "DESC NULLS LAST":
				b.WriteByte(' ')
				b.WriteString(s)
			}
		}

		// 4) Length/Collate ignored for Oracle

		results = append(results, clause.Expr{SQL: b.String()})
	}
	return
}

// DropIndex DROP INDEX <name>
func (m Migrator) DropIndex(value interface{}, name string) error {
	ns := getNS(m.DB, m.Dialector)
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		// Normalize via schema (if defined), but still quote through Dialector.
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}
		dictName := ns.dictCasePart(name)

		return m.DB.Exec("DROP INDEX ?", clause.Column{Name: dictName, Raw: true}).Error
	})
}

// HasIndex USER_INDEXES / ALL_INDEXES with dictionary casing for table and index
func (m Migrator) HasIndex(value interface{}, name string) bool {
	ns := getNS(m.DB, m.Dialector)

	var exists int
	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name // trust parsed name
		}

		owner, tab, hasOwner := ns.dictQualifiedParts(stmt.Table)

		// dictionary form of the name: unquoted -> UPPER(name), quoted -> exact inner
		dictName := ns.dictCasePart(name)

		if hasOwner {
			return m.DB.Raw(
				`SELECT 1 FROM ALL_INDEXES
				  WHERE OWNER = :owner AND TABLE_NAME = :tab AND INDEX_NAME = :idx AND ROWNUM = 1`,
				sql.Named("owner", owner),
				sql.Named("tab", tab),
				sql.Named("idx", dictName),
			).Scan(&exists).Error
		}
		return m.DB.Raw(
			`SELECT 1 FROM USER_INDEXES
			  WHERE TABLE_NAME = :tab AND INDEX_NAME = :idx AND ROWNUM = 1`,
			sql.Named("tab", tab),
			sql.Named("idx", dictName),
		).Scan(&exists).Error
	})
	return exists == 1
}

// RenameIndex ALTER INDEX <old> RENAME TO <new>
func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		// Resolve from schema if present for determinism
		if idx := stmt.Schema.LookIndex(oldName); idx != nil {
			oldName = idx.Name
		}
		// Build with placeholders so Dialector.QuoteTo is applied
		return m.DB.Exec(
			"ALTER INDEX ? RENAME TO ?",
			clause.Column{Name: oldName},
			clause.Column{Name: newName},
		).Error
	})
}

var onUpdateRe = regexp.MustCompile(`(?i)\s+ON\s+UPDATE\s+(NO\s+ACTION|RESTRICT|CASCADE|SET\s+NULL|SET\s+DEFAULT)`)

func stripOnUpdate(s string) string {
	return onUpdateRe.ReplaceAllString(s, "")
}

// getNS returns the configured oracle NamingStrategy (pointer), regardless of how it was set.
func getNS(db *gorm.DB, d gorm.Dialector) *NamingStrategy {
	if od, ok := d.(*Dialector); ok && od.namingStrategy != nil {
		return od.namingStrategy
	}
	if ns, ok := db.NamingStrategy.(*NamingStrategy); ok && ns != nil {
		return ns
	}

	return &NamingStrategy{PreferredCase: ScreamingSnakeCase, IdentifierMaxLength: 128}
}

// commentLiteral escapes a comment string for Oracle: single quotes doubled.
func commentLiteral(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for _, r := range s {
		if r == '\'' {
			b.WriteString("''")
		} else {
			b.WriteRune(r)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

// setColumnComment issues: COMMENT ON COLUMN <table>.<column> IS '<comment>'
// - <table> may be owner-qualified; QuoteTo handles dotted identifiers.
// - Replaces existing comment if present.
func (m Migrator) setColumnComment(table, column, comment string) error {
	if strings.TrimSpace(comment) == "" {
		return nil // nothing to do
	}
	var rawSql strings.Builder
	rawSql.WriteString("COMMENT ON COLUMN ")
	// dotted name: "<owner>"."<table>"."<column>" via QuoteTo
	m.DB.Dialector.QuoteTo(&rawSql, table+"."+column)
	rawSql.WriteString(" IS ")
	rawSql.WriteString(commentLiteral(comment))
	return m.DB.Exec(rawSql.String()).Error
}

// setTableComment issues: COMMENT ON TABLE <table> IS '<comment>'
func (m Migrator) setTableComment(table, comment string) error {
	if strings.TrimSpace(comment) == "" {
		return nil
	}
	var rawSql strings.Builder
	rawSql.WriteString("COMMENT ON TABLE ")
	m.DB.Dialector.QuoteTo(&rawSql, table)
	rawSql.WriteString(" IS ")
	rawSql.WriteString(commentLiteral(comment))
	return m.DB.Exec(rawSql.String()).Error
}

func isLOBOrLong(dt string) bool {
	u := strings.ToUpper(dt)
	return strings.Contains(u, "CLOB") || strings.Contains(u, "BLOB") || strings.Contains(u, "NCLOB") || strings.Contains(u, "LONG")
}

// crude detection: are we trying to change INTO a LOB/LONG?
func targetIsLOB(dt string) bool { return isLOBOrLong(dt) }
