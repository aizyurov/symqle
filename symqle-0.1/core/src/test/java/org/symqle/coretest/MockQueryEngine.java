package org.symqle.coretest;

import junit.framework.Assert;
import org.symqle.common.Callback;
import org.symqle.common.Query;
import org.symqle.common.SqlContext;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class MockQueryEngine<T> extends Assert implements QueryEngine {
    private final SqlContext sqlContext;
    private final List<T> resultSet;
    private final String statement;
    private final Option[] options;

    public MockQueryEngine(final SqlContext sqlContext, final List<T> resultSet, final String statement, final Option[] options) {
        this.sqlContext = sqlContext;
        this.resultSet = resultSet;
        this.statement = statement;
        this.options = options;
    }

    @Override
    public SqlContext initialContext() {
        return sqlContext;
    }

    @Override
    public <R> int scroll(final Query<R> query, final Callback<R> callback, final Option... options) throws SQLException {
        assertEquals(statement, query.sql());
        assertEquals(Arrays.asList(this.options), Arrays.asList(options));
        int count = 0;
        for (T result: resultSet) {
            count ++;
            if (!callback.iterate((R) result)) {
                break;
            }
        }
        return count;
    }
}
