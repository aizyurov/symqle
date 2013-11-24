package org.symqle.coretest;

import org.symqle.common.*;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;

import java.sql.SQLException;
import java.util.List;

/**
 * @author lvovich
 */
public class MockRowsQueryEngine extends AbstractMockEngine implements QueryEngine {

    private final List<Row> expectedRows;

    public MockRowsQueryEngine(final String statement, final SqlParameters parameters, final SqlContext sqlContext, final List<Row> expectedRows, final Option... options) {
        super(statement, parameters, sqlContext, options);
        this.expectedRows = expectedRows;
    }

    @Override
    public int scroll(final Sql query, final Callback<Row> callback, final Option... options) throws SQLException {
        verify(query, options);
        int count = 0;
        for (Row row: expectedRows) {
            count += 1;
            if (!callback.iterate(row)) {
                break;
            }
        }
        return count;
    }
}
