package org.symqle.dialect;

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
        return concat(SqlTerm.CAST.toSql(), SqlTerm.LEFT_PAREN.toSql(), e, SqlTerm.AS.toSql(), SqlTerm.BOOLEAN.toSql(), SqlTerm.RIGHT_PAREN.toSql());
    }

    @Override
    public Sql ValueExpression_is_BooleanExpression(final Sql bve) {
        // derby dialect misunderstands usage of BooleanExpression where ValueExpression is required;
        // surrounding with parentheses to avoid it
        return concat(SqlTerm.LEFT_PAREN.toSql(), bve, SqlTerm.RIGHT_PAREN.toSql());
    }

    @Override
    public Sql StringExpression_is_SUBSTRING_LEFT_PAREN_StringExpression_FROM_NumericExpression_FOR_NumericExpression_RIGHT_PAREN(final Sql s, final Sql start, final Sql len) {
        return concat(new CustomSql("SUBSTR"), SqlTerm.LEFT_PAREN.toSql(), s, SqlTerm.COMMA.toSql(), start, SqlTerm.COMMA.toSql(), len, SqlTerm.RIGHT_PAREN.toSql());
    }

    @Override
    public Sql StringExpression_is_SUBSTRING_LEFT_PAREN_StringExpression_FROM_NumericExpression_RIGHT_PAREN(final Sql s, final Sql start) {
        return concat(new CustomSql("SUBSTR"), SqlTerm.LEFT_PAREN.toSql(), s, SqlTerm.COMMA.toSql(), start, SqlTerm.RIGHT_PAREN.toSql());
    }

    @Override
    public Sql NumericExpression_is_POSITION_LEFT_PAREN_StringExpression_IN_StringExpression_RIGHT_PAREN(final Sql pattern, final Sql source) {
        return concat(new CustomSql("LOCATE"), SqlTerm.LEFT_PAREN.toSql(), pattern, SqlTerm.COMMA.toSql(), source, SqlTerm.RIGHT_PAREN.toSql());
    }
}
