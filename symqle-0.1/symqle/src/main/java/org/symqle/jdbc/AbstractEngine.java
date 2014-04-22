/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Sql;
import org.symqle.common.Parameterizer;
import org.symqle.common.Row;
import org.symqle.sql.Dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements common functionality of Engine.
 * @author lvovich
 */
public abstract class AbstractEngine extends AbstractQueryEngine implements Engine {

    /**
     * Constructs the engine
     * @param dialect forces the use of this dialect, no auto-detection
     * @param databaseName used for dialect detection
     * @param options options to apply for query building and execution
     */
    public AbstractEngine(final Dialect dialect, final String databaseName, final Option[] options) {
        super(dialect, databaseName, options);
    }

    public AbstractEngine(final String databaseName, final Option[] options) {
        super(databaseName, options);
    }

    /**
     * Open a connection to database.
     * Subclasses may re-use already open connection.
     * @return open connection.
     * @throws SQLException failure to open connection
     */
    protected abstract Connection getConnection() throws SQLException;

    /**
     * Release open connection.
     * Close or return to a pool or other object from which it was borrowed.
     * @param connection open connection
     * @throws SQLException error closing connection
     */
    protected abstract void releaseConnection(Connection connection) throws SQLException;

    @Override
    public int execute(final Sql statement, final List<Option> options) throws SQLException {
        return execute(statement, null, options);
    }

    @Override
    public int execute(final Sql statement, final GeneratedKeys<?> keyHolder, final List<Option> options) throws SQLException {
        final Connection connection = getConnection();
        try {
            final PreparedStatement preparedStatement = keyHolder != null ?
                    connection.prepareStatement(statement.text(), Statement.RETURN_GENERATED_KEYS)
                    : connection.prepareStatement(statement.text());
            try {
                setupStatement(preparedStatement, statement, options);
                statement.setParameters(new StatementParameters(preparedStatement));
                int affectedRows = preparedStatement.executeUpdate();
                if (keyHolder != null) {
                    final ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                    try {
                        generatedKeys.next();
                        final ResultSetRow row = new ResultSetRow(generatedKeys, new InnerQueryEngine(this, connection));
                        keyHolder.read(row.getValue(1));
                    } finally {
                        generatedKeys.close();
                    }
                }
                return affectedRows;
            } finally {
                preparedStatement.close();
            }

        } finally {
            releaseConnection(connection);
        }
    }

    @Override
    public int scroll(final Sql query, final Callback<Row> callback, final List<Option> options) throws SQLException {
        final Connection connection = getConnection();
        try {
            return scroll(connection, query, callback, options);
        } finally {
            releaseConnection(connection);
        }
    }

    @Override
    public Batcher newBatcher(final int batchSizeLimit) {
        return new BatcherImpl(batchSizeLimit);
    }

    private static class StatementKey {
        private final String statementText;
        private final List<Option> options;

        private StatementKey(final String statementText, final List<Option> options) {
            this.statementText = statementText;
            this.options = options;
        }

        public boolean sameAs(final StatementKey other) {
            return other != null && statementText.equals(other.statementText)
                && options.equals(other.options);
        }
    }

    private class BatcherImpl implements Batcher {
        private final int batchSize;
        private final List<Parameterizer> queue;
        private volatile StatementKey currentKey = null;

        private BatcherImpl(final int batchSize) {
            this.batchSize = batchSize;
            this.queue = new ArrayList<Parameterizer>(batchSize);
        }

        @Override
        public synchronized int[] submit(final Sql sql, final List<Option> options) throws SQLException {
            int[] rowsAffected = new int[0];

            final StatementKey newKey = new StatementKey(sql.text(), options);
            if (queue.size() >= batchSize || !newKey.sameAs(currentKey)) {
                rowsAffected = flush();
            }
            queue.add(new SavedParameterizer(sql));
            currentKey = newKey;
            return rowsAffected;
        }

        @Override
        public synchronized int[] flush() throws SQLException {
            if (queue.size() == 0) {
                return new int[0];
            }
            final Connection connection = getConnection();
            try {
                return flush(connection);
            } finally {
                releaseConnection(connection);
            }
        }

        private int[] flush(final Connection connection) throws SQLException {
            try {
                final PreparedStatement preparedStatement = connection.prepareStatement(currentKey.statementText);
                try {
                    for (Option option : currentKey.options) {
                        option.apply(preparedStatement);
                    }
                    for (Parameterizer queued : queue) {
                        queued.setParameters(new StatementParameters(preparedStatement));
                        preparedStatement.addBatch();
                    }
                    return preparedStatement.executeBatch();
                } finally {
                    preparedStatement.close();
                }
            } finally {
                queue.clear();
            }
        }

        @Override
        public Engine getEngine() {
            return AbstractEngine.this;
        }
    }
}
