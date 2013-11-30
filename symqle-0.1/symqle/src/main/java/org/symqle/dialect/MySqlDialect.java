package org.symqle.dialect;

import org.symqle.common.Sql;
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
    public Sql SelectStatement_is_QueryExpression_FOR_READ_ONLY(Sql qe) {
        return SelectStatement_is_QueryExpression(qe);
    }

    @Override
    public String fallbackTableName() {
        return null;
    }

    @Override
    public Sql ValueExpression_is_BooleanExpression(final Sql bve) {
        // mysql dialect misunderstands usage of BooleanExpression where ValueExpression is required in construction
        // WHERE T.x IS NOT NULL LIKE '0'
        // surrounding with parentheses to avoid it
        return concat(SqlTerm.LEFT_PAREN.toSql(), bve, SqlTerm.RIGHT_PAREN.toSql());
    }

}
