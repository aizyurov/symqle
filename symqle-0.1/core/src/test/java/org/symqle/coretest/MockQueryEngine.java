package org.symqle.coretest;

import org.symqle.common.*;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;

import java.sql.SQLException;
import java.util.List;

/**
 * @author lvovich
 */
public class MockQueryEngine extends AbstractMockEngine implements QueryEngine {
    private final List<Row> resultSet;

    public MockQueryEngine(final SqlContext sqlContext, final List<Row> resultSet, final String statement, final SqlParameters parameters, final Option... options) {
        super(statement, parameters, sqlContext, options);
        this.resultSet = resultSet;
    }

    @Override
    public int scroll(final Sql query, final Callback<Row> callback, final Option... options) throws SQLException {
        verify(query, options);
        int count = 0;
        for (Row result: resultSet) {
            count ++;
            if (!callback.iterate(result)) {
                break;
            }
        }
        return count;
    }

}
