package org.simqle.derby;

import org.simqle.CustomSql;
import org.simqle.Sql;
import org.simqle.SqlTerm;
import org.simqle.sql.Dialect;
import org.simqle.sql.GenericDialect;

/**
 * @author lvovich
 */
public class DerbyDialect extends GenericDialect {

    private DerbyDialect() {
    }

    private static Dialect instance = new DerbyDialect();

    public static Dialect get() {
        return instance;
    }

    @Override
    public Sql FromClauseFromNothing() {
        return new CustomSql("FROM (VALUES(1)) dummy");
    }

    @Override
    public Sql BooleanPrimary_is_ValueExpressionPrimary(final Sql e) {
        return concat(SqlTerm.CAST, SqlTerm.LEFT_PAREN, e, SqlTerm.AS, SqlTerm.BOOLEAN, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public Sql ValueExpressionPrimary_is_LEFT_PAREN_ValueExpression_RIGHT_PAREN(final Sql e) {
        // prevent double parentesizing
        final String sqlText = e.getSqlText();
        // prevent double parenthesizing
        if (sqlText.startsWith("(") && sqlText.endsWith(")")) {
            return e;
        } else {
            return concat(SqlTerm.LEFT_PAREN, e, SqlTerm.RIGHT_PAREN);
        }
    }



    @Override
    public Sql ValueExpression_is_BooleanExpression(final Sql bve) {
        // derby dialect misunderstands usage of BooleanExpression where ValueExpression is required;
        // surrounding with parentheses to avoid it
        final String sqlText = bve.getSqlText();
        // prevent double parenthesizing
        if (sqlText.startsWith("(") && sqlText.endsWith(")")) {
            return bve;
        } else {
            return concat(SqlTerm.LEFT_PAREN, bve, SqlTerm.RIGHT_PAREN);
        }
    }
}
