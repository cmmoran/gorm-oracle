package oracle

import (
	"strings"

	"github.com/emirpasic/gods/v2/sets/hashset"
)

var ReservedWords = hashset.New[string](ReservedWordsList...)

func IsReservedWord(v string) bool {
	parts := strings.Split(v, " ")

	return ReservedWords.Contains(parts...)
}

var ReservedWordsList = []string{
	"ACCESS", "ELSE", "MODIFY", "START",
	"ADD", "EXCLUSIVE", "NOAUDIT", "SELECT",
	"ALL", "EXISTS", "NOCOMPRESS", "SESSION",
	"ALTER", "FILE", "NOT", "SET",
	"AND", "FLOAT", "NOTFOUND", "SHARE",
	"ANY", "FOR", "NOWAIT", "SIZE",
	"ARRAYLEN", "FROM", "NULL", "SMALLINT",
	"AS", "GRANT", "NUMBER", "SQLBUF",
	"ASC", "GROUP", "OF", "SUCCESSFUL",
	"AUDIT", "HAVING", "OFFLINE", "SYNONYM",
	"BETWEEN", "IDENTIFIED", "ON", "SYSDATE",
	"BY", "IMMEDIATE", "ONLINE", "TABLE",
	"CHAR", "IN", "OPTION", "THEN",
	"CHECK", "INCREMENT", "OR", "TO",
	"CLUSTER", "INDEX", "ORDER", "TRIGGER",
	"COLUMN", "INITIAL", "PCTFREE", "UID",
	"COMMENT", "INSERT", "PRIOR", "UNION",
	"COMPRESS", "INTEGER", "PRIVILEGES", "UNIQUE",
	"CONNECT", "INTERSECT", "PUBLIC", "UPDATE",
	"CREATE", "INTO", "RAW", "USER",
	"CURRENT", "IS", "RENAME", "VALIDATE",
	"DATE", "LEVEL", "RESOURCE", "VALUES",
	"DECIMAL", "LIKE", "REVOKE", "VARCHAR",
	"DEFAULT", "LOCK", "ROW", "VARCHAR2",
	"DELETE", "LONG", "ROWID", "VIEW",
	"DESC", "MAXEXTENTS", "ROWLABEL", "WHENEVER",
	"DISTINCT", "MINUS", "ROWNUM", "WHERE",
	"DROP", "MODE", "ROWS", "WITH",
}
