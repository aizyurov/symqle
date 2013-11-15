package org.symqle.jdbc;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author lvovich
 */
class DataSourceConnector implements Connector {

    private final DataSource dataSource;

    public DataSourceConnector(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
