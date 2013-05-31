package org.simqle.mysql;

import org.simqle.CustomSql;
import org.simqle.Sql;
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
    public Sql StringExpression_is_StringExpression_CONCAT_ValueExpressionPrimary(final Sql s, final Sql e) {
        throw new IllegalStateException("concat operator is not supported by this dialect; use concat function instead");
    }

    @Override
    public Sql FromClauseFromNothing() {
        return new CustomSql("");
    }
}
