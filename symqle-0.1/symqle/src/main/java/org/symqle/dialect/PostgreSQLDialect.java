package org.symqle.dialect;

import org.symqle.common.Sql;
import org.symqle.querybuilder.SqlTerm;
import org.symqle.sql.GenericDialect;

import static org.symqle.querybuilder.SqlTerm.FULL;
import static org.symqle.querybuilder.SqlTerm.JOIN;
import static org.symqle.querybuilder.SqlTerm.ON;
import static org.symqle.querybuilder.SqlTerm.OUTER;

/**
 * @author lvovich
 */
public class PostgreSQLDialect extends GenericDialect {

    @Override
    public String getName() {
        return "PostgreSQL";
    }

    @Override
    public Sql Predicand_is_StringExpression(final Sql e) {
        return concat(SqlTerm.LEFT_PAREN.toSql(), e, SqlTerm.RIGHT_PAREN.toSql());
    }

    @Override
    public Sql Predicate_is_LikePredicateBase_ESCAPE_StringExpression(final Sql b, final Sql esc) {
        return concat(SqlTerm.LEFT_PAREN.toSql(), super.Predicate_is_LikePredicateBase_ESCAPE_StringExpression(b, esc), SqlTerm.RIGHT_PAREN.toSql());
    }

    @Override
    public Sql Predicate_is_LikePredicateBase(final Sql b) {
        return concat(SqlTerm.LEFT_PAREN.toSql(), super.Predicate_is_LikePredicateBase(b), SqlTerm.RIGHT_PAREN.toSql());
    }

    public Sql BooleanPrimary_is_ValueExpressionPrimary(final Sql e) {
        return concat(SqlTerm.CAST.toSql(), SqlTerm.LEFT_PAREN.toSql(), e, SqlTerm.AS.toSql(), SqlTerm.BOOLEAN.toSql(), SqlTerm.RIGHT_PAREN.toSql());
    }

    public Sql BooleanPrimary_is_Predicate(final Sql p) {
        return concat(SqlTerm.LEFT_PAREN.toSql(), super.BooleanPrimary_is_Predicate(p), SqlTerm.RIGHT_PAREN.toSql());
    }



    @Override
    public String fallbackTableName() {
        return "(VALUES(1))";
    }

    public Sql TableReference_is_TableReference_OUTER_JOIN_TableReference_ON_BooleanExpression(final Sql l, final Sql r, final Sql c) {
        return concat(l, FULL.toSql(), OUTER.toSql(), JOIN.toSql(), r, ON.toSql(), c);
    }


}
