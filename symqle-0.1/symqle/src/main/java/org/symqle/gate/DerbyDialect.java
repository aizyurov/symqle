package org.symqle.gate;

import org.symqle.common.Sql;
import org.symqle.querybuilder.CustomSql;
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

    @Override
    public Sql StringExpression_is_SUBSTRING_LEFT_PAREN_StringExpression_FROM_NumericExpression_FOR_NumericExpression_RIGHT_PAREN(final Sql s, final Sql start, final Sql len) {
        return concat(new CustomSql("SUBSTR"), SqlTerm.LEFT_PAREN, s, SqlTerm.COMMA, start, SqlTerm.COMMA, len, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public Sql StringExpression_is_SUBSTRING_LEFT_PAREN_StringExpression_FROM_NumericExpression_RIGHT_PAREN(final Sql s, final Sql start) {
        return concat(new CustomSql("SUBSTR"), SqlTerm.LEFT_PAREN, s, SqlTerm.COMMA, start, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public Sql NumericExpression_is_POSITION_LEFT_PAREN_StringExpression_IN_StringExpression_RIGHT_PAREN(final Sql pattern, final Sql source) {
        return concat(new CustomSql("LOCATE"), SqlTerm.LEFT_PAREN, pattern, SqlTerm.COMMA, source, SqlTerm.RIGHT_PAREN);
    }
}
