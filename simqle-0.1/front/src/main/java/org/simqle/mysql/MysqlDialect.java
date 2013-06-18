package org.simqle.mysql;

import org.simqle.CustomSql;
import org.simqle.Sql;
import org.simqle.SqlTerm;
import org.simqle.sql.Dialect;
import org.simqle.sql.GenericDialect;

/**
 * @author lvovich
 */
public class MysqlDialect extends GenericDialect {

    private final static Dialect instance = new MysqlDialect();

    private MysqlDialect() {
    }

    public static Dialect get() {
        return instance;
    }

    /**
     * Mysql does not support FOR READ ONLY; just omitted
      * @param cspec
     * @return
     */
    public Sql SelectStatement_is_CursorSpecification_FOR_READ_ONLY(final Sql cspec) {
        return SelectStatement_is_CursorSpecification(cspec);
    }

    @Override
    public Sql FromClauseFromNothing() {
        return new CustomSql("");
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
        // mysql dialect misunderstands usage of BooleanExpression where ValueExpression is required;
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
