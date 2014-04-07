package org.symqle.dialect;

import org.symqle.common.Sql;
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
    public Sql Predicand_is_StringExpression(final Sql e) {
        return concat(SqlTerm.LEFT_PAREN, e, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public Sql Predicate_is_LikePredicateBase_ESCAPE_StringExpression(final Sql b, final Sql esc) {
        return concat(SqlTerm.LEFT_PAREN, super.Predicate_is_LikePredicateBase_ESCAPE_StringExpression(b, esc), SqlTerm.RIGHT_PAREN);
    }

    @Override
    public Sql Predicate_is_LikePredicateBase(final Sql b) {
        return concat(SqlTerm.LEFT_PAREN, super.Predicate_is_LikePredicateBase(b), SqlTerm.RIGHT_PAREN);
    }

    // IS FALSE .. IS TRUE have too high priority in PostgreSQL
    // A = B IS TRUE is interpreted as A= ( B IS TRUE )
    // as a workaround, parenthesise argument

    @Override
    public Sql BooleanTest_is_BooleanPrimary_IS_FALSE(final Sql bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, FALSE);
    }

    @Override
    public Sql BooleanTest_is_BooleanPrimary_IS_NOT_FALSE(final Sql bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, NOT, FALSE);
    }

    @Override
    public Sql BooleanTest_is_BooleanPrimary_IS_NOT_TRUE(final Sql bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, NOT, TRUE);
    }

    @Override
    public Sql BooleanTest_is_BooleanPrimary_IS_NOT_UNKNOWN(final Sql bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, NOT, UNKNOWN);
    }

    @Override
    public Sql BooleanTest_is_BooleanPrimary_IS_TRUE(final Sql bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, TRUE);
    }

    @Override
    public Sql BooleanTest_is_BooleanPrimary_IS_UNKNOWN(final Sql bp) {
        return concat(LEFT_PAREN, bp, RIGHT_PAREN, IS, UNKNOWN);
    }


    // mostly for DynamicParameter:: its type is unknown, need explicit CAST

    public Sql BooleanPrimary_is_ValueExpressionPrimary(final Sql e) {
        return concat(SqlTerm.CAST, SqlTerm.LEFT_PAREN, e, SqlTerm.AS, SqlTerm.BOOLEAN, SqlTerm.RIGHT_PAREN);
    }

    @Override
    public String fallbackTableName() {
        return "(VALUES(1))";
    }

    public Sql TableReference_is_TableReference_OUTER_JOIN_TableReference_ON_BooleanExpression(final Sql l, final Sql r, final Sql c) {
        return concat(l, FULL, OUTER, JOIN, r, ON, c);
    }


}
