package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.Query;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;

import java.sql.SQLException;
import java.util.List;

/**
 * @author lvovich
 */
public class MockQueryEngine<T> extends AbstractMockEngine implements QueryEngine {
    private final List<T> resultSet;

    public MockQueryEngine(final SqlContext sqlContext, final List<T> resultSet, final String statement, final SqlParameters parameters, final Option... options) {
        super(statement, parameters, sqlContext, options);
        this.resultSet = resultSet;
    }

    @Override
    public <R> int scroll(final Query<R> query, final Callback<R> callback, final Option... options) throws SQLException {
        verify(query, options);
        int count = 0;
        for (T result: resultSet) {
            count ++;
            if (!callback.iterate((R) result)) {
                break;
            }
        }
        return count;
    }

    protected List<T> getExpected() {
        return resultSet;
    }

}
