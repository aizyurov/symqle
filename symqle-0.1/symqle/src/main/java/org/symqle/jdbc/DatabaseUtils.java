package org.symqle.jdbc;

import org.symqle.dialect.DerbyDialect;
import org.symqle.dialect.MySqlDialect;
import org.symqle.sql.Dialect;
import org.symqle.sql.GenericDialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author lvovich
 */
public final class DatabaseUtils {

    public static String getDatabaseName(final DataSource dataSource) throws SQLException {
        final Connection connection = dataSource.getConnection();
        try {
            final DatabaseMetaData metaData = connection.getMetaData();
            return metaData.getDatabaseProductName();
        } finally {
            connection.close();
        }
    }

    private final static Dialect[] knownDialects = {
            new DerbyDialect(),
            new MySqlDialect()
    };

    private final static ConnectorWrapper[] KNOWN_WRAPPERs = {
            new MySqlConnectorWrapper()
    };

    public static Dialect getDialect(final String databaseName) {
        for (Dialect dialect : knownDialects) {
            if (dialect.getName().equals(databaseName)) {
                return dialect;
            }
        }
        return new GenericDialect();
    }

    public static ConnectorWrapper getConnectorWrapperFactory(final String databaseName) {
        for (ConnectorWrapper factory : KNOWN_WRAPPERs) {
            if (factory.getName().equals(databaseName)) {
                return factory;
            }
        }
        return null;
    }



}
