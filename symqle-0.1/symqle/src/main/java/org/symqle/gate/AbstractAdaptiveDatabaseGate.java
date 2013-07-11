package org.symqle.gate;

import org.symqle.jdbc.Option;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.Dialect;
import org.symqle.sql.GenericDialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * @author lvovich
 */
public abstract class AbstractAdaptiveDatabaseGate implements DatabaseGate {

    protected abstract Connection connect() throws SQLException;

    private Dialect dialect;
    private ConnectionCallback connectionCallback;
    private String databaseName;

    protected AbstractAdaptiveDatabaseGate() {
    }

    protected AbstractAdaptiveDatabaseGate(final Dialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public final synchronized Connection getConnection() throws SQLException {
        final Connection connection = connect();
        if (connectionCallback == null) {
            connectionCallback = findConnectionCallback();
        }
        connectionCallback.call(connection);
        return connection;
    }

    @Override
    public synchronized Dialect getDialect() {
        if (dialect == null) {
            try {
                dialect = findDialect();
            } catch (SQLException e) {
                throw new IllegalStateException("Cannot determine dialect due to SQLException", e);
            }
        }
        return dialect;
    }

    public String getDatabaseName() throws SQLException {
        if (databaseName == null) {
            final Connection connection = connect();
            try {
                final DatabaseMetaData metaData = connection.getMetaData();
                databaseName = metaData.getDatabaseProductName();
            } finally {
                connection.close();
            }
        }
        return databaseName;
    }

    private Dialect findDialect() throws SQLException {
        for (Dialect dialect: knownDialects) {
            if (getDatabaseName().equals(dialect.getName())) {
                return dialect;
            }
        }
        return GenericDialect.get();
    }

    private ConnectionCallback findConnectionCallback() throws SQLException {
        for (ConnectionCallbackFactory factory : knownConnectionCallbackFactories) {
            if (getDatabaseName().equals(factory.getName())) {
                return factory.createCallback();
            }
        }
        return new NullConnectionCallback();
    }

    @Override
    public abstract List<Option> getOptions();

    public final Dialect[] knownDialects = {
            DerbyDialect.get(),
            MySqlDialect.get()
    };

    private ConnectionCallbackFactory[] knownConnectionCallbackFactories = {
        new MySqlConnectionCallbackFactory()
    };

    private static class NullConnectionCallback implements ConnectionCallback {
        @Override
        public void call(final Connection connection) {
            // do nothing
        }
    }
}
