package org.symqle.dialect;

import org.symqle.common.SqlBuilder;
import org.symqle.querybuilder.StringSqlBuilder;
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
    public SqlBuilder BooleanPrimary_is_ValueExpressionPrimary(final SqlBuilder e) {
        return concat(SqlTerm.CAST, SqlTerm.LEFT_PAREN, e, SqlTerm.AS, SqlTerm.BOOLEAN, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public SqlBuilder ValueExpression_is_BooleanExpression(final SqlBuilder bve) {
        // derby dialect misunderstands usage of BooleanExpression where ValueExpression is required;
        // surrounding with parentheses to avoid it
        return concat(SqlTerm.LEFT_PAREN, bve, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public SqlBuilder StringExpression_is_SUBSTRING_LEFT_PAREN_StringExpression_FROM_NumericExpression_FOR_NumericExpression_RIGHT_PAREN(final SqlBuilder s, final SqlBuilder start, final SqlBuilder len) {
        return concat(new StringSqlBuilder("SUBSTR"), SqlTerm.LEFT_PAREN, s, SqlTerm.COMMA, start, SqlTerm.COMMA, len, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public SqlBuilder StringExpression_is_SUBSTRING_LEFT_PAREN_StringExpression_FROM_NumericExpression_RIGHT_PAREN(final SqlBuilder s, final SqlBuilder start) {
        return concat(new StringSqlBuilder("SUBSTR"), SqlTerm.LEFT_PAREN, s, SqlTerm.COMMA, start, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public SqlBuilder NumericExpression_is_POSITION_LEFT_PAREN_StringExpression_IN_StringExpression_RIGHT_PAREN(final SqlBuilder pattern, final SqlBuilder source) {
        return concat(new StringSqlBuilder("LOCATE"), SqlTerm.LEFT_PAREN, pattern, SqlTerm.COMMA, source, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public SqlBuilder NumericExpression_is_CHAR_LENGTH_LEFT_PAREN_StringExpression_RIGHT_PAREN(final SqlBuilder string) {
        return concat(SqlTerm.LENGTH, SqlTerm.LEFT_PAREN, string, SqlTerm.RIGHT_PAREN);
    }
}
