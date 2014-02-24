package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.Row;
import org.symqle.common.Sql;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Batcher;
import org.symqle.jdbc.Engine;
import org.symqle.jdbc.Option;
import org.symqle.sql.ColumnName;
import org.symqle.sql.Dialect;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class MockEngine extends AbstractMockEngine implements Engine {

    final int affectedRows;
    final Object returnedKey;

    /**
     *
     * @param affectedRows return value for execute and submit
     * @param returnedKey return value for executeReturnKey
     * @param statement expected statement
     * @param parameters mock object for parameters to call
     * @param sqlContext initial context
     * @param options expected execute/submit options
     */
    public MockEngine(final int affectedRows, final Object returnedKey, final String statement, final SqlParameters parameters, final SqlContext sqlContext, final Option... options) {
        super(statement, parameters, sqlContext, options);
        this.affectedRows = affectedRows;
        this.returnedKey = returnedKey;
    }

    @Override
    public int execute(final Sql sql, final Option... options) throws SQLException {
        verify(sql, options);
        return affectedRows;
    }

    /**
     * Returns the first element of the list provided in the constructor
     * @param sql the SQL to execute
     * @param keyColumn the column, for which key is generated
     * @param options  statement options
     * @param <R>
     * @return
     * @throws SQLException
     */
    @Override
    public <R> R executeReturnKey(final Sql sql, final ColumnName<R> keyColumn, final Option... options) throws SQLException {
        verify(sql, options);
        return (R) returnedKey;
    }

    public Batcher newBatcher(int ignored) {
        return new Batcher() {
            @Override
            public int[] submit(final Sql sql, final Option... options) throws SQLException {
                verify(sql, options);
                final int[] result = new int[affectedRows];
                Arrays.fill(result, 1);
                return result;
            }

            @Override
            public int[] flush() throws SQLException {
                final int[] result = new int[affectedRows];
                Arrays.fill(result, 1);
                return result;
            }

            @Override
            public Dialect getDialect() {
                return MockEngine.this.getDialect();
            }

            @Override
            public List<Option> getOptions() {
                return MockEngine.this.getOptions();
            }
        };
    }



    @Override
    public int scroll(final Sql query, final Callback<Row> callback, final Option... options) throws SQLException {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

}
