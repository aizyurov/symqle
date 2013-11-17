package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Mapper;
import org.symqle.common.Query;
import org.symqle.common.Sql;
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
    private int batchSize = 100;
    private final List<Sql> queue = new ArrayList<Sql>(batchSize);
    private volatile StatementKey currentKey = null;

    public AbstractEngine(final Dialect dialect, final String databaseName, final Option[] options) {
        super(dialect, databaseName, options);
    }

    public AbstractEngine(final String databaseName, final Option[] options) {
        super(databaseName, options);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int setBatchSize(final int batchSize) throws SQLException {
        if (batchSize <=0) {
            throw new IllegalArgumentException("batchSize should be positive, actual "+batchSize);
        }
        this.batchSize = batchSize;
        if (queue.size() >= batchSize) {
            return flush();
        } else {
            return Engine.NOTHING_FLUSHED;
        }
    }

    @Override
    public int execute(final Sql statement, final Option... options) throws SQLException {
        final Connection connection = getConnection();
        try {
            flushInConnection(connection);
            final PreparedStatement preparedStatement = connection.prepareStatement(statement.sql());
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
            flushInConnection(connection);
            final PreparedStatement preparedStatement = connection.prepareStatement(statement.sql(), Statement.RETURN_GENERATED_KEYS);
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
    public <T> int scroll(final Query<T> query, final Callback<T> callback, final Option... options) throws SQLException {
        final Connection connection = getConnection();
        try {
            flushInConnection(connection);
            return scrollInConnection(connection, query, callback, options);
        } finally {
            releaseConnection(connection);
        }
    }

    @Override
    public int flush() throws SQLException {
        final Connection connection = getConnection();
        try {
            return flushInConnection(connection);
        } finally {
            releaseConnection(connection);
        }
    }

    private int flushInConnection(final Connection connection) throws SQLException {
        if (queue.size() == 0) {
            return Engine.NOTHING_FLUSHED;
        }
        try {
            final PreparedStatement preparedStatement = connection.prepareStatement(currentKey.statement.sql());
            try {
                setupStatement(preparedStatement, currentKey.statement, currentKey.options);
                for (Sql queued : queue) {
                    queued.setParameters(new StatementParameters(preparedStatement));
                    preparedStatement.addBatch();
                }
                final int[] results = preparedStatement.executeBatch();
                int total = 0;
                for (int i = 0 ; i < results.length ; i++) {
                    final int result = results[i];
                    if (result <0) {
                        return result;
                    } else {
                        total += result;
                    }
                }
                return total;
            } finally {
                preparedStatement.close();
            }
        } finally {
            queue.clear();
        }
    }

    @Override
    public int submit(final Sql statement, final Option... options) throws SQLException {
        int rowsAffected = 0;
        final StatementKey newKey = new StatementKey(statement, options);
        if (queue.size() >= batchSize || !newKey.equals(currentKey)) {
            rowsAffected = flush();
        }
        queue.add(statement);
        currentKey = newKey;
        return rowsAffected;
    }

    @Override
    public void execute(final ConnectionCallback callback)  throws SQLException {
        final Connection connection = getConnection();
        try {
            flushInConnection(connection);
            callback.call(connection);
        } finally {
            releaseConnection(connection);
        }
    }

    private static class StatementKey {
        private final Sql statement;
        private final Option[] options;

        private StatementKey(final Sql statement, final Option[] options) {
            this.statement = statement;
            this.options = options;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final StatementKey that = (StatementKey) o;

            if (!statement.sql().equals(that.statement.sql())) return false;
            if (!Arrays.equals(options, that.options)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = statement.sql().hashCode();
            result = 31 * result + Arrays.hashCode(options);
            return result;
        }
    }
}
