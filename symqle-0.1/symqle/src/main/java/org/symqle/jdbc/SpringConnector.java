package org.symqle.jdbc;

import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author lvovich
 */
class SpringConnector implements Connector {

    private final DataSource dataSource;

    public SpringConnector(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return DataSourceUtils.getConnection(dataSource);
    }
}
