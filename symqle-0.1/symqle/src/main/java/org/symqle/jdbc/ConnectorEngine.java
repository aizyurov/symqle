package org.symqle.jdbc;

import org.symqle.sql.Dialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author lvovich
 */
public class ConnectorEngine extends AbstractEngine {

    private final Connector connector;

//    protected ConnectorEngine(final Connector connector, final Dialect dialect, final String databaseName, final Option[] options) {
//        super(dialect, databaseName, options);
//        this.connector = connector;
//    }

    public ConnectorEngine(final DataSource dataSource, final Dialect dialect, final Option... options) throws SQLException {
        super(dialect, DatabaseUtils.getDatabaseName(dataSource), options);
        final Connector connector = new DataSourceConnector(dataSource);
        this.connector = DatabaseUtils.wrap(connector, getDatabaseName());
    }

    public ConnectorEngine(final DataSource dataSource, final Option... options) throws SQLException {
        super(DatabaseUtils.getDatabaseName(dataSource), options);
        final Connector connector = new DataSourceConnector(dataSource);
        this.connector = DatabaseUtils.wrap(connector, getDatabaseName());
    }

    @Override
    protected final Connection getConnection() throws SQLException {
        return connector.getConnection();
    }

    @Override
    protected final void releaseConnection(final Connection connection) throws SQLException {
        connection.close();
    }

}
