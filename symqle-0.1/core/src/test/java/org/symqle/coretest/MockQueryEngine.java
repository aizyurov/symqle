package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.Row;
import org.symqle.common.Sql;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class MockQueryEngine extends AbstractMockEngine implements QueryEngine {
    private final List<Row> resultSet;

    public MockQueryEngine(final SqlContext sqlContext, final List<Row> resultSet, final String statement, final SqlParameters parameters, final List<Option> options) {
        super(statement, parameters, sqlContext, options);
        this.resultSet = resultSet;
    }

    public MockQueryEngine(final SqlContext sqlContext, final List<Row> resultSet, final String statement, final SqlParameters parameters) {
        this(sqlContext, resultSet, statement, parameters, Collections.<Option>emptyList());
    }

    @Override
    public int scroll(final Sql query, final Callback<Row> callback, final List<Option> options) throws SQLException {
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
