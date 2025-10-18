package oracle

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/jinzhu/inflection"
	"gorm.io/gorm/schema"

	"github.com/iancoleman/strcase"
)

// Replacer replacer interface like strings.Replacer
type Replacer interface {
	Replace(name string) string
}

var _ schema.Namer = (*NamingStrategy)(nil)

type Case int

const (
	ScreamingSnakeCase Case = iota
	SnakeCase
	CamelCase
)

// NamingStrategy tables, columns naming strategy
type NamingStrategy struct {
	TablePrefix         string
	SingularTable       bool
	NameReplacer        Replacer
	IdentifierMaxLength int

	PreferredCase          Case // default is SCREAMING_SNAKE_CASE
	NamingCaseSensitive    bool // whether naming is case-sensitive
	capIdentifierMaxLength int
}

// TableName convert string to table name
func (ns *NamingStrategy) TableName(str string) string {
	// Resolve maxLength without mutating receiver
	maxLength := ns.IdentifierMaxLength
	if maxLength <= 0 {
		maxLength = ns.capIdentifierMaxLength
	}

	qualifiers := make([]qualifier, 0, 3)

	// 1) Handle TablePrefix:
	//    - If it contains a dot (schema-qualified), push its parts as qualifiers.
	//    - Else, treat as a literal prefix to the base table name (not a qualifier).
	var (
		schemaQuals []string
		basePrefix  string
	)
	if p := strings.TrimSpace(ns.TablePrefix); p != "" {
		if strings.Contains(p, ".") {
			// schema-qualified prefix (e.g., ACME. or "Acme"."Core".)
			schemaQuals = splitQualified(strings.TrimSuffix(p, "."))
		} else {
			// simple string prefix (e.g., "t_" or "tbl_")
			basePrefix = p
		}
	}

	// push schemaQuals (if any)
	for _, q := range schemaQuals {
		n, quoted := ns.normalizePart(q)
		qualifiers = append(qualifiers, qualifier{name: n, quoted: quoted})
	}

	// 2) Compute base logical name:
	//    If explicitly quoted -> DO NOT pluralize; keep exact inner.
	//    Else pluralize when SingularTable == false.
	if inner, ok := IsExplicitQuoted(str); ok {
		// exact name, no pluralization
		str = `"` + inner + `"`
	} else if !ns.SingularTable {
		str = inflection.Plural(str)
	}

	// 3) Apply simple (non-schema) prefix to base logical name before normalization.
	//    Preserve explicit quotes if present.
	if basePrefix != "" {
		if inner, ok := IsExplicitQuoted(str); ok {
			str = `"` + basePrefix + inner + `"`
		} else {
			str = basePrefix + str
		}
	}

	// 4) Normalize the final base name and append as the last qualifier.
	baseName, quoted := ns.normalizePart(str)
	qualifiers = append(qualifiers, qualifier{name: baseName, quoted: quoted})

	// 5) Join with quoting/capping.
	//    Use maxLength we computed (do not mutate ns in a value receiver).
	//    joinQualified already calls shortenIfNeeded; adapt it to accept maxLength if needed.
	return ns.joinQualified(qualifiers)
}

// SchemaName returns the normalized OWNER/SCHEMA portion for a possibly-qualified table.
// If no explicit owner is provided, returns "".
func (ns *NamingStrategy) SchemaName(table string) string {
	parts := splitQualified(table)
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		// unqualified; no explicit schema/owner
		return ""
	}
	owner, quoted := ns.normalizePart(parts[0])
	return ns.joinQualified([]qualifier{{name: owner, quoted: quoted}})
}

// ColumnName convert string to column name
func (ns *NamingStrategy) ColumnName(_ /*table*/, column string) string {
	// Explicitly quoted tag: column:"\"weirdName\"" → DBName = weirdName
	if inner, ok := IsExplicitQuoted(column); ok {
		return inner
	}

	switch ns.PreferredCase {
	case ScreamingSnakeCase:
		// We avoid quotes unless required; Oracle will store as UPPERCASE when unquoted.
		return strcase.ToScreamingSnake(column)
	case SnakeCase:
		// You emit quoted exact snake_case in DDL, but DBName should be the inner literal.
		return strcase.ToSnake(column)
	case CamelCase:
		// Same: DBName is inner literal; quoting is a SQL rendering concern.
		return strcase.ToCamel(column)
	default:
		return column
	}
}

// JoinTableName applies the same rules as TableName for join tables.
func (ns *NamingStrategy) JoinTableName(joinTable string) string {
	return ns.TableName(joinTable)
}

// RelationshipFKName builds a deterministic FK constraint name honoring Oracle's 30-byte cap.
// We generate an unqualified, safe token (A–Z, 0–9, _) and let QuoteTo add quotes only if required elsewhere.
func (ns *NamingStrategy) RelationshipFKName(rel schema.Relationship) string {
	// base table
	var baseTable string
	if rel.JoinTable != nil && rel.JoinTable.Table != "" {
		baseTable = rel.JoinTable.Table
	} else if rel.FieldSchema != nil && rel.FieldSchema.Table != "" {
		baseTable = rel.FieldSchema.Table
	} else if rel.Schema != nil && rel.Schema.Table != "" {
		baseTable = rel.Schema.Table
	} else {
		baseTable = rel.Name
	}

	// collect columns (prefer ForeignKey; else PrimaryKey)
	cols := make([]string, 0, len(rel.References))
	for _, ref := range rel.References {
		switch {
		case ref.ForeignKey != nil && ref.ForeignKey.DBName != "":
			cols = append(cols, ref.ForeignKey.DBName)
		case ref.PrimaryKey != nil && ref.PrimaryKey.DBName != "":
			cols = append(cols, ref.PrimaryKey.DBName)
		}
	}
	if len(cols) == 0 {
		cols = []string{rel.Name}
	}

	// stable ordering for composite keys
	sort.Strings(cols)

	return ns.genToken("FK", baseTable, strings.Join(cols, "_"))
}

// CheckerName builds a CHECK constraint name: CK_<TABLE>_<COLUMN...>, capped to Oracle limits.
func (ns *NamingStrategy) CheckerName(table, column string) string {
	return ns.genToken("CK", table, column)
}

// IndexName builds a unique index name(table, hint) -> IDX_<TABLE>_<HINT>_<FNV8>, capped to IdentifierMaxLength
func (ns *NamingStrategy) IndexName(table, column string) string {
	return ns.genToken("IDX", table, column)
}

// UniqueName builds a unique index/constraint name: UK_<TABLE>_<COLUMN...>, capped to Oracle limits.
func (ns *NamingStrategy) UniqueName(table, column string) string {
	return ns.genToken("UK", table, column)
}

// region -------------------- helpers for generated identifiers --------------------

// genToken returns an Oracle-safe, schema-unique name like KIND_<TABLE>[...HASH].
// It disambiguates quoted vs. unquoted twins by hashing the *dictionary-case*
// (OWNER, OBJECT, COLS...). It also respects Oracle's 30/128-byte limit.
func (ns *NamingStrategy) genToken(kind string, tableOrObject string, cols ...string) string {
	// Defaults
	maxLength := ns.IdentifierMaxLength
	if maxLength <= 0 {
		maxLength = ns.capIdentifierMaxLength
	}

	// 1) Dictionary-case anchor (handles quoted vs unquoted correctly)
	owner, object, _ := ns.dictQualifiedParts(tableOrObject) // object: exact if quoted, UPPER if unquoted

	// 2) Human-readable base: KIND_<OBJECT> (UPPER_SNAKE for safety)
	baseObj := ns.toCase(object)
	base := kind + "_" + baseObj
	for _, c := range cols {
		base += "_" + ns.toCase(c)
	}

	// 3) Build uniqueness seed across schema + object + columns (also in dictionary-case)
	var seed strings.Builder
	seed.WriteString(owner)
	seed.WriteByte('.')
	seed.WriteString(object)
	for _, c := range cols {
		seed.WriteByte('|')
		seed.WriteString(ns.dictCasePart(c))
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(seed.String()))
	suffix := fmt.Sprintf("_%08X", h.Sum32()) // 9 chars including underscore

	name := base
	if len(name) <= maxLength {
		return name
	}

	// Trim the object portion first, keep KIND_ and the hash suffix
	// Total len = len(kind) + 1 + len(trimmedObj) + len(suffix)
	maxObj := maxLength - (len(kind) + 1 + len(suffix))
	if maxObj < 1 {
		// Pathological: fall back to KIND_<HASH>, and truncate if still too long
		name = kind + suffix
		if len(name) > maxLength {
			return name[:maxLength]
		}
		return name
	}
	if maxObj > len(baseObj) {
		maxObj = len(baseObj)
	}
	return kind + "_" + baseObj[:maxObj] + suffix
}

// endregion

// region ---------- helpers: case transforms ----------

// IsSafeOracleUnquoted
//
// Unquoted identifiers:
// - are stored uppercase
// - must begin with a letter
// - may contain A–Z, 0–9, _, $, #
// - must not be a reserved word
//
// Input s must already be in its target case for the chosen mode.
//
// Returns true if s can be emitted unquoted safely.
func IsSafeOracleUnquoted(s string) bool {
	if s == "" {
		return false
	}
	r0 := rune(s[0])
	if !('A' <= r0 && r0 <= 'Z') {
		return false
	}
	for _, r := range s {
		switch {
		case 'A' <= r && r <= 'Z':
		case '0' <= r && r <= '9':
		case r == '_' || r == '$' || r == '#':
		default:
			return false
		}
	}
	up := strings.ToUpper(s)
	if IsReservedWord(up) {
		return false
	}
	return true
}

// IsExplicitQuoted Detects explicit user-quoted literal: (example: "Name")
func IsExplicitQuoted(part string) (inner string, ok bool) {
	if len(part) >= 2 && part[0] == '"' && part[len(part)-1] == '"' {
		return part[1 : len(part)-1], true
	}
	return "", false
}

// Split OWNER.TABLE (preserves quotes)
func splitQualified(input string) []string {
	var parts []string
	var b strings.Builder
	quoted := false
	for _, r := range input {
		switch r {
		case '"':
			quoted = !quoted
			b.WriteRune(r)
		case '.':
			if quoted {
				b.WriteRune(r)
			} else {
				parts = append(parts, b.String())
				b.Reset()
			}
		default:
			b.WriteRune(r)
		}
	}
	if b.Len() > 0 {
		parts = append(parts, b.String())
	}
	return parts
}

type qualifier struct {
	name   string
	quoted bool
}

func (ns *NamingStrategy) joinQualified(parts []qualifier) string {
	maxLength := ns.IdentifierMaxLength
	if maxLength <= 0 {
		maxLength = ns.capIdentifierMaxLength
	}
	var out strings.Builder
	for i, p := range parts {
		if i > 0 {
			out.WriteByte('.')
		}
		name := ns.shortenWithMax(p.name, maxLength)
		if p.quoted {
			out.WriteByte('"')
			for _, r := range name {
				if r == '"' {
					out.WriteString(`""`)
				} else {
					out.WriteRune(r)
				}
			}
			out.WriteByte('"')
		} else {
			out.WriteString(name)
		}
	}
	return out.String()
}

// FNV-1a suffix for capped names (Oracle 30-byte max)
func (ns *NamingStrategy) shortenIfNeeded(s string) string {
	maxLength := ns.IdentifierMaxLength
	if maxLength <= 0 {
		maxLength = ns.capIdentifierMaxLength
	}
	return ns.shortenWithMax(s, maxLength)
}

func (ns *NamingStrategy) shortenWithMax(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	sum := h.Sum32()
	const hexdigits = "0123456789ABCDEF"
	hexValue := func(v uint32) string {
		b := make([]byte, 8)
		for i := 7; i >= 0; i-- {
			b[i] = hexdigits[v&0xF]
			v >>= 4
		}
		return string(b)
	}(sum)
	const sufLen = 1 + 8
	if maxLen <= sufLen {
		return hexValue
	}
	return s[:maxLen-sufLen] + "_" + hexValue
}

func (ns *NamingStrategy) toCase(part string) string {
	switch ns.PreferredCase {
	case ScreamingSnakeCase:
		// Already uppercased by ToScreamingSnake; ensures A_Z0_9 underscores.
		return strcase.ToScreamingSnake(part)
	case SnakeCase:
		return strcase.ToSnake(part)
	case CamelCase:
		return strcase.ToCamel(part)
	default:
		return part
	}
}

// endregion

// region ---------- Normalization according to PreferredCase + NamingCaseSensitive ----------
//
// Rules:
//
//	ScreamingSnakeCase:
//	  - if NamingCaseSensitive=false  -> emit UNQUOTED UPPER_SNAKE (always)
//	  - if NamingCaseSensitive=true   -> avoid quotes unless required; use UPPER_SNAKE when unquoted
//	SnakeCase or CamelCase: coerce to quoted exact case always.
//
// Explicit quotes in the input (tags like column:"\"Weird\"") are honored: always quoted exact.
func (ns *NamingStrategy) normalizePart(part string) (name string, quoted bool) {
	// Explicitly quoted literal -> preserve exact, always quoted
	if inner, ok := IsExplicitQuoted(part); ok {
		return inner, true
	}

	switch ns.PreferredCase {
	case ScreamingSnakeCase:
		canon := ns.toCase(part) // already UPPER_SNAKE
		if !ns.NamingCaseSensitive {
			// always unquoted UPPER_SNAKE unless reserved (then quote)
			if IsSafeOracleUnquoted(canon) {
				return canon, false
			}
			return canon, true
		}
		// namingCaseSensitive==true -> avoid quotes unless required
		if IsSafeOracleUnquoted(canon) {
			return canon, false
		}
		return canon, true

	case SnakeCase, CamelCase:
		// Coerce to quoted exact snake_case
		return ns.toCase(part), true

	default:
		// Defensive: fall back to unquoted upper
		up := strings.ToUpper(part)
		return up, false
	}
}

// Apply normalization to each dotted part
func (ns *NamingStrategy) normalizeQualified(ident string) string {
	if ident == "" {
		return ""
	}
	raw := splitQualified(ident)
	out := make([]qualifier, 0, len(raw))
	for _, p := range raw {
		n, q := ns.normalizePart(p)
		out = append(out, qualifier{name: n, quoted: q})
	}
	return ns.joinQualified(out)
}

// endregion

// region ---------- Dictionary-case helpers (for Migrator queries) ----------

// dictCasePart returns the value to compare against Oracle's data dictionary
// without recasing opaque tokens (e.g., hash suffixes). If the identifier
// would be unquoted, return UPPER(s). If it would be quoted, return s exact.
func (ns NamingStrategy) dictCasePart(s string) string {
	// honor explicit quotes like "\"Weird\""
	if inner, ok := IsExplicitQuoted(s); ok {
		return inner // dictionary stores quoted identifiers case-sensitively
	}

	// Decide if we *would* emit quotes for this part, but do NOT recase `s`.
	switch ns.PreferredCase {
	case SnakeCase, CamelCase:
		// these modes always quote -> exact case
		return s

	case ScreamingSnakeCase:
		// avoid quotes unless required; only check safety on UPPER(s)
		up := strings.ToUpper(s)
		if IsSafeOracleUnquoted(up) {
			return up // dictionary matches unquoted as UPPER
		}
		return s // would be quoted -> exact
	default:
		// defensive fallback: treat as unquoted
		return strings.ToUpper(s)
	}
}

// Returns (owner, object, hasOwner)
func (ns *NamingStrategy) dictQualifiedParts(ident string) (owner, object string, hasOwner bool) {
	raw := splitQualified(ident)
	switch len(raw) {
	case 0:
		return "", "", false
	case 1:
		return "", ns.dictCasePart(raw[0]), false
	default:
		return ns.dictCasePart(raw[0]),
			ns.dictCasePart(raw[1]), true
	}
}

// endregion

// region ---------- Literal helpers ----------
// Minimal literal rendering for defaults.
func toSQLLiteral(v interface{}) string {
	switch x := v.(type) {
	case bool:
		if x {
			return "1"
		}
		return "0"
	case int:
		return fmt.Sprintf("%d", x)
	case int8, int16, int32, int64:
		return fmt.Sprintf("%d", x)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x)
	case float32, float64:
		return fmt.Sprintf("%v", x)
	default:
		s := fmt.Sprintf("%v", x)
		var b strings.Builder
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
}

func isUniqueClass(s string) bool { return strings.Contains(strings.ToUpper(s), "UNIQUE") }

// endregion
