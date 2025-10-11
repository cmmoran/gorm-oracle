package oracle

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

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

	PreferredCase       Case // default is SCREAMING_SNAKE_CASE
	NamingCaseSensitive bool // whether naming is case-sensitive
}

// TableName convert string to table name
func (ns NamingStrategy) TableName(str string) (name string) {
	prefix := ""
	if len(ns.TablePrefix) > 0 {
		prefix = ns.TablePrefix + "_"
	}
	name = prefix + ns.applyNameReplacer(str)
	if !ns.SingularTable {
		name = prefix + inflection.Plural(ns.applyNameReplacer(str))
	}

	return ns.normalizeQualifiedIdent(name)
}

// SchemaName generate schema name from table name, don't guarantee it is the reverse value of TableName
func (ns NamingStrategy) SchemaName(table string) (name string) {
	name = strings.TrimPrefix(table, ns.TablePrefix)

	if !ns.SingularTable {
		name = inflection.Singular(table)
	}

	return ns.normalizeQualifiedIdent(name)
}

// ColumnName convert string to column name
func (ns NamingStrategy) ColumnName(_, column string) (name string) {
	return ns.normalizeQualifiedIdent(ns.applyNameReplacer(column))
}

// JoinTableName convert string to join table name
func (ns NamingStrategy) JoinTableName(str string) (name string) {
	prefix := ""
	if len(ns.TablePrefix) > 0 {
		prefix = ns.TablePrefix + "_"
	}

	name = prefix + ns.applyNameReplacer(str, true)
	if !ns.SingularTable {
		name = prefix + inflection.Plural(ns.applyNameReplacer(str, true))
	}

	return ns.normalizeQualifiedIdent(name)
}

// RelationshipFKName generate fk name for relation
func (ns NamingStrategy) RelationshipFKName(rel schema.Relationship) (name string) {
	table := rel.Schema.Table
	if IsQuoted(table) {
		table = strings.Trim(table, `"`)
		return quote(ns.formatName("fk", table, ns.applyNameReplacer(rel.Name, true)))
	}

	return ns.normalizeQualifiedIdent(ns.formatName("fk", table, ns.applyNameReplacer(rel.Name, true)))
}

// CheckerName generate checker name
func (ns NamingStrategy) CheckerName(table, column string) (name string) {
	if IsQuoted(table) {
		table = strings.Trim(table, `"`)
		return quote(ns.formatName("chk", table, ns.applyNameReplacer(column, true)))
	}
	return ns.normalizeQualifiedIdent(ns.formatName("chk", table, ns.applyNameReplacer(column, true)))
}

// IndexName generate index name
func (ns NamingStrategy) IndexName(table, column string) (name string) {
	if IsQuoted(table) {
		table = strings.Trim(table, `"`)
		return quote(ns.formatName("idx", table, ns.applyNameReplacer(column, true)))
	}
	return ns.normalizeQualifiedIdent(ns.formatName("idx", table, ns.applyNameReplacer(column, true)))
}

// UniqueName generate unique constraint name
func (ns NamingStrategy) UniqueName(table, column string) (name string) {
	if IsQuoted(table) {
		table = strings.Trim(table, `"`)
		return quote(ns.formatName("uni", table, ns.applyNameReplacer(column, true)))
	}
	return ns.normalizeQualifiedIdent(ns.formatName("uni", table, ns.applyNameReplacer(column, true)))
}

func (ns NamingStrategy) formatName(prefix, table, name string) string {
	formattedName := strings.ReplaceAll(strings.Join([]string{
		prefix, table, name,
	}, "_"), ".", "_")

	if ns.IdentifierMaxLength == 0 {
		ns.IdentifierMaxLength = 64
	}

	if utf8.RuneCountInString(formattedName) > ns.IdentifierMaxLength {
		h := sha1.New()
		h.Write([]byte(formattedName))
		bs := h.Sum(nil)

		formattedName = formattedName[0:ns.IdentifierMaxLength-8] + hex.EncodeToString(bs)[:8]
	}
	return formattedName
}

var (
	// https://github.com/golang/lint/blob/master/lint.go#L770
	commonInitialisms = []string{"API", "ASCII", "CPU", "CSS", "DNS", "EOF", "GUID", "HTML", "HTTP", "HTTPS", "ID", "IP", "JSON", "LHS", "QPS", "RAM", "RHS", "RPC", "SLA", "SMTP", "SSH", "TLS", "TTL", "UID", "UI", "UUID", "URI", "URL", "UTF8", "VM", "XML", "XSRF", "XSS"}
)

func init() {
	for _, initialism := range commonInitialisms {
		strcase.ConfigureAcronym(initialism, initialism)
	}
}

func (ns NamingStrategy) applyNameReplacer(name string, unquote ...any) string {
	removeQuotes := len(unquote) > 0

	if name == "" {
		return ""
	}
	if IsQuoted(name) {
		if removeQuotes {
			name = name[1 : len(name)-1]
		}
		return name
	}
	if IsReservedWord(name) {
		name = quoteIdent(name)
	}

	if ns.NameReplacer != nil {
		tmpName := ns.NameReplacer.Replace(name)

		if tmpName == "" {
			return name
		}

		name = tmpName
	}

	return name
}

// Assumes you already have a Dialector interface type in this package
// that exposes NamingCaseSensitive (as in your earlier snippets).

// normalizeIdent transforms a single Oracle identifier according to the
// Dialector's NamingCaseSensitive setting and Oracle rules.
// - If already quoted → passthrough exactly.
// - If NamingCaseSensitive == false (Oracle-default semantics):
//   - Identifier must be representable unquoted (^[A-Za-z][A-Za-z0-9_$#]{0,29}$), else error.
//   - Fold to UPPERCASE.
//   - If it is a reserved word → quote it (preserve original spelling inside quotes).
//
// - If NamingCaseSensitive == true:
//   - If valid unquoted and not reserved → leave unquoted as-is.
//   - Otherwise → quote (preserve original spelling; escape inner quotes).
//
// Returns the rendered identifier (possibly quoted), a flag indicating whether it’s quoted, or an error.
func (ns NamingStrategy) normalizeIdent(in string) (string, bool, error) {
	name := strings.TrimSpace(in)
	if name == "" {
		return "", false, fmt.Errorf("empty identifier")
	}

	// If caller already quoted → never touch casing or content.
	if IsQuoted(name) {
		return name, true, nil
	}

	ignoreCase := !ns.NamingCaseSensitive

	// Case-insensitive (Oracle default) path.
	if ignoreCase {
		if !validUnquoted(name) {
			return "", false, fmt.Errorf("identifier %q not representable unquoted with NamingCaseSensitive=false", name)
		}
		up := strings.ToUpper(name)
		if IsReservedWord(up) {
			return quoteIdent(up), true, nil // preserve original spelling inside quotes
		}
		up = ns.toPreferredCase(name)
		return up, false, nil
	}

	// Case-sensitive path.
	upper := strings.ToUpper(name)
	if validUnquoted(name) && !IsReservedWord(upper) {
		name = ns.toPreferredCase(name)
		return name, false, nil // leave as-is, unquoted
	}
	return quoteIdent(upper), true, nil
}

func (ns NamingStrategy) toPreferredCase(in string) string {
	switch ns.PreferredCase {
	case ScreamingSnakeCase:
		return strcase.ToScreamingSnake(in)
	case SnakeCase:
		return strcase.ToSnake(in)
	case CamelCase:
		return strcase.ToCamel(in)
	}

	return in
}

// normalizeQualifiedIdent applies normalizeIdent to each part of a schema-qualified name.
// Example inputs: "schema.table", "table".
// If any part errors (e.g., invalid in NamingCaseSensitive=false), the function returns the original input.
func (ns NamingStrategy) normalizeQualifiedIdent(in string) string {
	parts := strings.Split(strings.TrimSpace(in), ".")
	if len(parts) == 0 {
		return in
	}
	out := make([]string, len(parts))
	for i, p := range parts {
		frag, _, err := ns.normalizeIdent(p)
		if err != nil {
			return in // return original on error
		}
		out[i] = frag
	}
	return strings.Join(out, ".")
}

// --- helpers ---

// Oracle unquoted identifier: starts with letter; then letters/digits/_ $ #; max 30 chars.
var validUnquotedRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_$#]{0,64}$`)

func validUnquoted(s string) bool {
	return validUnquotedRE.MatchString(s)
}

// quoteIdent wraps s in double quotes and escapes any embedded double quotes.
func quoteIdent(s string) string {
	if IsQuoted(s) {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quote(s string) string {
	if IsQuoted(s) {
		return s
	}
	s = strings.ReplaceAll(s, `"`, "")
	return fmt.Sprintf(`"%s"`, s)
}

// IsQuoted returns true if s is already a double-quoted SQL identifier.
func IsQuoted(s string) bool {
	return len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"'
}
