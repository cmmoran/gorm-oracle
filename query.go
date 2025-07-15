package oracle

import (
	"reflect"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// FixOracleQueryVars walks the WHERE clause and replaces any var binds and replaces them with oracle friendly ones
func FixOracleQueryVars(db *gorm.DB) {
	stmt := db.Statement
	c, ok := stmt.Clauses[clause.Where{}.Name()]
	if !ok {
		if len(stmt.Vars) > 0 && len(stmt.SQL.String()) > 0 {
			for i, v := range stmt.Vars {
				stmt.Vars[i] = replaceIdExpressionVars(v)
			}
		}
		return
	}
	if se, seOk := c.Expression.(clause.Set); seOk {
		for i, asn := range se {
			se[i].Value = replaceIdExpressionVars(asn.Value)
		}
	}
	if wh, whOk := c.Expression.(clause.Where); whOk {
		for i, expr := range wh.Exprs {
			wh.Exprs[i] = replaceExpr(replaceIdExpressionVars, expr)
		}
	}
}

func replaceIdExpressionVars(from any) any {
	if from == nil {
		return from
	}

	fromV := reflect.ValueOf(from)
	for fromV.Kind() == reflect.Ptr {
		fromV = fromV.Elem()
	}
	if isSixteenByteType(fromV.Type()) {
		if b, err := toBytesFrom16Array(from); err == nil {
			return b
		}
	}
	return from
}

func replaceExpr(tf func(any) any, expr clause.Expression) clause.Expression {
	switch x := expr.(type) {
	case clause.AndConditions:
		for i, v := range x.Exprs {
			x.Exprs[i] = replaceExpr(tf, v)
		}
	case clause.OrConditions:
		for i, v := range x.Exprs {
			x.Exprs[i] = replaceExpr(tf, v)
		}
	case clause.NotConditions:
		for i, v := range x.Exprs {
			x.Exprs[i] = replaceExpr(tf, v)
		}
	case clause.NamedExpr:
		for i, v := range x.Vars {
			x.Vars[i] = tf(v)
		}
	case clause.Expr:
		for i, v := range x.Vars {
			x.Vars[i] = tf(v)
		}
	case clause.Eq:
		if x.Value != nil {
			x.Value = tf(x.Value)
		}
	case clause.Neq:
		if x.Value != nil {
			x.Value = tf(x.Value)
		}
	case clause.Gt:
		if x.Value != nil {
			x.Value = tf(x.Value)
		}
	case clause.Gte:
		if x.Value != nil {
			x.Value = tf(x.Value)
		}
	case clause.Lt:
		if x.Value != nil {
			x.Value = tf(x.Value)
		}
	case clause.Lte:
		if x.Value != nil {
			x.Value = tf(x.Value)
		}
	case clause.IN:
		for i, v := range x.Values {
			x.Values[i] = tf(v)
		}
	default:
		return expr
	}
	return expr
}
