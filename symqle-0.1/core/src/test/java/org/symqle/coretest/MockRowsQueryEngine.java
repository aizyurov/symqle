package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.Sql;
import org.symqle.common.Row;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;

import java.sql.SQLException;
import java.util.List;

/**
 * @author lvovich
 */
public class MockRowsQueryEngine extends AbstractMockEngine implements QueryEngine {

    private final List<Row> expectedRows;

    public MockRowsQueryEngine(final String statement, final SqlParameters parameters, final SqlContext sqlContext, final List<Row> expectedRows, final List<Option> options) {
        super(statement, parameters, sqlContext, options);
        this.expectedRows = expectedRows;
    }

    @Override
    public int scroll(final Sql query, final Callback<Row> callback, final List<Option> options) throws SQLException {
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
