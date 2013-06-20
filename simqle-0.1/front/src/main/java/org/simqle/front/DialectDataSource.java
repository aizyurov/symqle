package org.simqle.front;

import org.simqle.sql.DatabaseGate;
import org.simqle.sql.Dialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author lvovich
 */
public class DialectDataSource implements DatabaseGate {
    private final DataSource dataSource;
    private final Dialect dialect;
    private final String connectionSetup;

    public DialectDataSource(final Dialect dialect, final DataSource dataSource, final String connectionSetup) {
        this.dialect = dialect;
        this.dataSource = dataSource;
        this.connectionSetup = connectionSetup;
    }

    public DialectDataSource(final Dialect dialect, final DataSource dataSource) {
        this(dialect, dataSource, null);
    }

    public Connection getConnection() throws SQLException {
        final Connection connection = dataSource.getConnection();
        if (connectionSetup != null) {
            final Statement statement = connection.createStatement();
            try {
                statement.executeUpdate(connectionSetup);
            } finally {
                statement.close();
            }
        }
        return connection;
    }

    public Dialect getDialect() {
        return dialect;
    }

}
