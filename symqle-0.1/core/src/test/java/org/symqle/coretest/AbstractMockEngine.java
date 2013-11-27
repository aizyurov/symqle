package org.symqle.coretest;

import junit.framework.Assert;
import org.symqle.common.Sql;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;

import java.sql.SQLException;
import java.util.Arrays;

/**
 * @author lvovich
 */
public class AbstractMockEngine extends Assert {
    private SqlContext sqlContext;
    private final String statement;
    private final Option[] options;
    private final SqlParameters parameters;

    public AbstractMockEngine(final String statement, final SqlParameters parameters, final SqlContext sqlContext, final Option... options) {
        this.statement = statement;
        this.sqlContext = sqlContext;
        this.options = options;
        this.parameters = parameters;
    }

    public SqlContext initialContext() {
        return sqlContext;
    }

    protected final void verify(final Sql query, final Option[] options)
            throws SQLException {
        StringBuilder builder = new StringBuilder();
        query.append(builder);
        assertEquals(statement, builder.toString());
        assertEquals(Arrays.asList(this.options), Arrays.asList(options));
        query.setParameters(parameters);
    }

    public String getDatabaseName() {
        return "mock";
    }

}
