package org.symqle.jdbc;

import org.symqle.sql.Dialect;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author lvovich
 */
public class CommonEngineFactory extends AbstractEngineFactory {

    public Engine create(DataSource dataSource, Option... options) throws SQLException {
        final String databaseName = getDatabaseName(dataSource);
        return new ConnectorEngine(getConnector(databaseName, dataSource), getDialect(databaseName), options);
    }

    public Engine create(DataSource dataSource, Dialect dialect, Option... options) throws SQLException {
        final String databaseName = getDatabaseName(dataSource);
        return new ConnectorEngine(getConnector(databaseName, dataSource), dialect, options);
    }


    @Override
    protected Connector createConnector(final DataSource dataSource) {
        return new DataSourceConnector(dataSource);
    }


}
