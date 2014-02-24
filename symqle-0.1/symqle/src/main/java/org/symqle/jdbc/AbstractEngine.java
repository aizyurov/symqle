package org.symqle.jdbc;

import org.symqle.common.*;
import org.symqle.sql.ColumnName;
import org.symqle.sql.Dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public abstract class AbstractEngine extends AbstractQueryEngine implements Engine {

    public AbstractEngine(final Dialect dialect, final String databaseName, final Option[] options) {
        super(dialect, databaseName, options);
    }

    public AbstractEngine(final String databaseName, final Option[] options) {
        super(databaseName, options);
    }

    protected abstract Connection getConnection() throws SQLException;

    protected abstract void releaseConnection(Connection connection) throws SQLException;

    @Override
    public int execute(final Sql statement, final Option... options) throws SQLException {
        final Connection connection = getConnection();
        try {
            final PreparedStatement preparedStatement = connection.prepareStatement(statement.toString());
            try {
                setupStatement(preparedStatement, statement, options);
                statement.setParameters(new StatementParameters(preparedStatement));
                return preparedStatement.executeUpdate();
            } finally {
                preparedStatement.close();
            }
        } finally {
            releaseConnection(connection);
        }
    }

    @Override
    public <T> T executeReturnKey(final Sql statement, final ColumnName<T> keyColumn, final Option... options) throws SQLException {
        final Connection connection = getConnection();
        try {
            final PreparedStatement preparedStatement = connection.prepareStatement(statement.toString(), Statement.RETURN_GENERATED_KEYS);
            try {
                setupStatement(preparedStatement, statement, options);
                statement.setParameters(new StatementParameters(preparedStatement));
                preparedStatement.executeUpdate();
                final ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                try {
                    generatedKeys.next();
                    final ResultSetRow row = new ResultSetRow(generatedKeys, new InnerQueryEngine(this, connection));
                    final Mapper<T> mapper = keyColumn.getMapper();
                    return mapper.value(row.getValue(1));
                } finally {
                    generatedKeys.close();
                }
            } finally {
                preparedStatement.close();
            }

        } finally {
            releaseConnection(connection);
        }
    }

    @Override
    public int scroll(final Sql query, final Callback<Row> callback, final Option... options) throws SQLException {
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
        private final Sql statement;
        private final Option[] options;

        private StatementKey(final Sql statement, final Option[] options) {
            this.statement = statement;
            this.options = options;
        }

        public boolean sameAs(final StatementKey other) {
            return other != null && statement.toString().equals(other.statement.toString())
                && Arrays.equals(options, other.options);
        }
    }

    private class BatcherImpl implements Batcher {
        private final int batchSize;
        private final List<Sql> queue;
        private volatile StatementKey currentKey = null;

        private BatcherImpl(final int batchSize) {
            this.batchSize = batchSize;
            this.queue = new ArrayList<Sql>(batchSize);
        }

        @Override
        public synchronized int[] submit(final Sql sql, final Option... options) throws SQLException {
            int[] rowsAffected = new int[0];
            final StatementKey newKey = new StatementKey(sql, options);
            if (queue.size() >= batchSize || !newKey.sameAs(currentKey)) {
                rowsAffected = flush();
            }
            queue.add(sql);
            currentKey = newKey;
            return rowsAffected;
        }

        @Override
        public synchronized int[] flush() throws SQLException {
            final Connection connection = getConnection();
            try {
                return flush(connection);
            } finally {
                releaseConnection(connection);
            }
        }

        private int[] flush(final Connection connection) throws SQLException {
            if (queue.size() == 0) {
                return new int[0];
            }
            try {
                final PreparedStatement preparedStatement = connection.prepareStatement(currentKey.statement.toString());
                try {
                    setupStatement(preparedStatement, currentKey.statement, currentKey.options);
                    for (Sql queued : queue) {
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
        public Dialect getDialect() {
            return AbstractEngine.this.getDialect();
        }

        @Override
        public List<Option> getOptions() {
            return AbstractEngine.this.getOptions();
        }
    }
}
