package org.symqle.jdbc;

import org.symqle.gate.DerbyDialect;
import org.symqle.gate.MySqlDialect;
import org.symqle.sql.Dialect;
import org.symqle.sql.GenericDialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author lvovich
 */
abstract class AbstractEngineFactory {
    private final Dialect[] knownDialects = {
            new DerbyDialect(),
            new MySqlDialect()
    };
    private final ConnectorWrapperFactory[] knownWrapperFactories = {
            new MySqlConnectorWrapperFactory()
    };

    protected final Dialect getDialect(final String databaseName) {
        for (Dialect dialect : knownDialects) {
            if (dialect.getName().equals(databaseName)) {
                return dialect;
            }
        }
        return new GenericDialect();
    }

    protected final Connector getConnector(final String databaseName, final DataSource dataSource) {
        final Connector connector = createConnector(dataSource);
        for (ConnectorWrapperFactory factory : knownWrapperFactories) {
            if (factory.getName().equals(databaseName)) {
                return factory.wrap(connector);
            }
        }
        return connector;
    }

    protected abstract Connector createConnector(DataSource dataSource);

    protected final String getDatabaseName(final DataSource dataSource) throws SQLException {
        final Connection connection = dataSource.getConnection();
        try {
            final DatabaseMetaData metaData = connection.getMetaData();
            return metaData.getDatabaseProductName();
        } finally {
            connection.close();
        }

    }
}
