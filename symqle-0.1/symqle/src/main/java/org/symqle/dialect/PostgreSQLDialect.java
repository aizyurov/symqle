package org.symqle.dialect;

import org.symqle.common.SqlBuilder;
import org.symqle.querybuilder.SqlTerm;
import org.symqle.sql.GenericDialect;

import static org.symqle.querybuilder.SqlTerm.*;

/**
 * @author lvovich
 */
public class PostgreSQLDialect extends GenericDialect {

    @Override
    public String getName() {
        return "PostgreSQL";
    }

    // A || B IS NULL is interpreted as A || ( B IS NULL )
    // parenthesise StringExpression when it is predicand

    @Override
    public SqlBuilder Predicand_is_StringExpression(final SqlBuilder e) {
        return concat(SqlTerm.LEFT_PAREN, e, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public SqlBuilder Predicate_is_LikePredicateBase_ESCAPE_StringExpression(final SqlBuilder b, final SqlBuilder esc) {
        return concat(SqlTerm.LEFT_PAREN, super.Predicate_is_LikePredicateBase_ESCAPE_StringExpression(b, esc), SqlTerm.RIGHT_PAREN);
    }

    @Override
    public SqlBuilder Predicate_is_LikePredicateBase(final SqlBuilder b) {
        return concat(SqlTerm.LEFT_PAREN, super.Predicate_is_LikePredicateBase(b), SqlTerm.RIGHT_PAREN);
    }

    // IS FALSE .. IS TRUE have too high priority in PostgreSQL
    // A = B IS TRUE is interpreted as A= ( B IS TRUE )
    // as a workaround, parenthesise argument

    @Override
    public SqlBuilder BooleanTest_is_BooleanPrimary_IS_FALSE(final SqlBuilder bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, FALSE);
    }

    @Override
    public SqlBuilder BooleanTest_is_BooleanPrimary_IS_NOT_FALSE(final SqlBuilder bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, NOT, FALSE);
    }

    @Override
    public SqlBuilder BooleanTest_is_BooleanPrimary_IS_NOT_TRUE(final SqlBuilder bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, NOT, TRUE);
    }

    @Override
    public SqlBuilder BooleanTest_is_BooleanPrimary_IS_NOT_UNKNOWN(final SqlBuilder bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, NOT, UNKNOWN);
    }

    @Override
    public SqlBuilder BooleanTest_is_BooleanPrimary_IS_TRUE(final SqlBuilder bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, TRUE);
    }

    @Override
    public SqlBuilder BooleanTest_is_BooleanPrimary_IS_UNKNOWN(final SqlBuilder bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, UNKNOWN);
    }


    // mostly for DynamicParameter:: its type is unknown, need explicit CAST

    public SqlBuilder BooleanPrimary_is_ValueExpressionPrimary(final SqlBuilder e) {
        return concat(SqlTerm.CAST, SqlTerm.LEFT_PAREN, e, SqlTerm.AS, SqlTerm.BOOLEAN, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public String fallbackTableName() {
        return "(VALUES(1))";
    }

    public SqlBuilder TableReference_is_TableReference_OUTER_JOIN_TableReference_ON_BooleanExpression(final SqlBuilder l, final SqlBuilder r, final SqlBuilder c) {
        return concat(l, FULL, OUTER, JOIN, r, ON, c);
    }


}
