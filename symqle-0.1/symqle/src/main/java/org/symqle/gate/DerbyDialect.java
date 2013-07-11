package org.symqle.gate;

import org.symqle.common.Sql;
import org.symqle.querybuilder.SqlTerm;
import org.symqle.sql.GenericDialect;

/**
 * @author lvovich
 */
public class DerbyDialect extends GenericDialect {

    @Override
    public String getName() {
        return "Apache Derby";
    }

    @Override
    public String fallbackTableName() {
        return "(VALUES(1))";
    }

    @Override
    public Sql BooleanPrimary_is_ValueExpressionPrimary(final Sql e) {
        return concat(SqlTerm.CAST, SqlTerm.LEFT_PAREN, e, SqlTerm.AS, SqlTerm.BOOLEAN, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public Sql ValueExpression_is_BooleanExpression(final Sql bve) {
        // derby dialect misunderstands usage of BooleanExpression where ValueExpression is required;
        // surrounding with parentheses to avoid it
        return concat(SqlTerm.LEFT_PAREN, bve, SqlTerm.RIGHT_PAREN);
    }
}
