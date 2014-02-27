package org.symqle.coretest;

import junit.framework.Assert;
import org.symqle.common.Sql;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;
import org.symqle.sql.Dialect;
import org.symqle.sql.GenericDialect;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class AbstractMockEngine extends Assert {
    private SqlContext sqlContext;
    private final String statement;
    private final List<Option> options;
    private final SqlParameters parameters;

    public AbstractMockEngine(final String statement, final SqlParameters parameters, final SqlContext sqlContext, final List<Option> options) {
        this.statement = statement;
        this.sqlContext = sqlContext;
        this.options = options;
        this.parameters = parameters;
    }

    public Dialect getDialect() {
        final Dialect contextDialect = sqlContext.get(Dialect.class);
        return contextDialect == null ? new GenericDialect() : contextDialect;
    }

    public List<Option> getOptions() {
        return Collections.emptyList();
    }
    protected final void verify(final Sql query, final List<Option> options)
            throws SQLException {
        StringBuilder builder = new StringBuilder();
        query.append(builder);
        assertEquals(statement, builder.toString());
        assertEquals(this.options, options);
        query.setParameters(parameters);
    }

    public String getDatabaseName() {
        return "mock";
    }

}
