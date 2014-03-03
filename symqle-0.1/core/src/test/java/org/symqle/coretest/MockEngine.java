package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.CompiledSql;
import org.symqle.common.Row;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Batcher;
import org.symqle.jdbc.Engine;
import org.symqle.jdbc.GeneratedKeys;
import org.symqle.jdbc.Option;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class MockEngine extends AbstractMockEngine implements Engine {

    final int affectedRows;

    /**
     *
     * @param affectedRows return value for execute and submit
     * @param statement expected statement
     * @param parameters mock object for parameters to call
     * @param sqlContext initial context
     * @param options expected execute/submit options
     */
    public MockEngine(final int affectedRows, final String statement, final SqlParameters parameters, final SqlContext sqlContext, final List<Option> options) {
        super(statement, parameters, sqlContext, options);
        this.affectedRows = affectedRows;
    }

    public MockEngine(final int affectedRows, final String statement, final SqlParameters parameters, final SqlContext sqlContext) {
        this(affectedRows, statement, parameters, sqlContext, Collections.<Option>emptyList());
    }

    @Override
    public int execute(final CompiledSql sql, final List<Option> options) throws SQLException {
        verify(sql, options);
        return affectedRows;
    }

    @Override
    public int execute(final CompiledSql sql, final GeneratedKeys<?> keys, final List<Option> options) throws SQLException {
        verify(sql, options);
        return affectedRows;
    }

    public Batcher newBatcher(int ignored) {
        return new Batcher() {
            @Override
            public int[] submit(final CompiledSql sql, final List<Option> options) throws SQLException {
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
            public Engine getEngine() {
                return MockEngine.this;
            }
        };
    }



    @Override
    public int scroll(final CompiledSql query, final Callback<Row> callback, final List<Option> options) throws SQLException {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

}
