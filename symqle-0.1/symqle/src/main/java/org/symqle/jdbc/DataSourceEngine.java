package org.symqle.jdbc;

import org.symqle.sql.Dialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author lvovich
 */
public class DataSourceEngine extends AbstractEngine {

    private final Connector connector;

    public DataSourceEngine(final DataSource dataSource, final Dialect dialect, final Option... options) throws SQLException {
        super(dialect, DatabaseUtils.getDatabaseName(dataSource), options);
        final Connector connector = new DataSourceConnector(dataSource);
        this.connector = DatabaseUtils.wrap(connector, getDatabaseName());
    }

    public DataSourceEngine(final DataSource dataSource, final Option... options) throws SQLException {
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
