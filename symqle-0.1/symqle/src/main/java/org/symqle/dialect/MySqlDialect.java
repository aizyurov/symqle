package org.symqle.dialect;

import org.symqle.common.SqlBuilder;
import org.symqle.common.StringSqlBuilder;
import org.symqle.querybuilder.SqlTerm;
import org.symqle.sql.GenericDialect;

/**
 * @author lvovich
 */
public class MySqlDialect extends GenericDialect {

    @Override
    public String getName() {
        return "MySQL";
    }

    /**
     * Mysql does not support FOR READ ONLY; just omitted
      * @param qe QueryExpression
     * @return
     */
    @Override
    public SqlBuilder SelectStatement_is_QueryExpression_FOR_READ_ONLY(SqlBuilder qe) {
        return SelectStatement_is_QueryExpression(qe);
    }

    @Override
    public String fallbackTableName() {
        return null;
    }

    @Override
    public SqlBuilder ValueExpression_is_BooleanExpression(final SqlBuilder bve) {
        // mysql dialect misunderstands usage of BooleanExpression where ValueExpression is required in construction
        // WHERE T.x IS NOT NULL LIKE '0'
        // surrounding with parentheses to avoid it
        return concat(SqlTerm.LEFT_PAREN, bve, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public SqlBuilder QueryExpression_is_QueryExpressionBasic_FETCH_FIRST_Literal_ROWS_ONLY(final SqlBuilder qe, final SqlBuilder limit) {
        return concat(qe, SqlTerm.LIMIT, new StringSqlBuilder("0"), SqlTerm.COMMA, limit);
    }

    @Override
    public SqlBuilder QueryExpression_is_QueryExpressionBasic_OFFSET_Literal_ROWS_FETCH_FIRST_Literal_ROWS_ONLY(final SqlBuilder qe, final SqlBuilder offset, final SqlBuilder limit) {
        return concat(qe, SqlTerm.LIMIT, offset, SqlTerm.COMMA, limit);
    }
}
