package org.symqle.integration.model;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.symqle.jdbc.DatabaseUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Properties;

/**
 * @author lvovich
 */
public class ExternalDbEnvironment extends AbstractTestEnvironment {

    @Override
    public DataSource prepareDataSource(Properties properties) throws Exception {
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(properties.getProperty("symqle.jdbc.url"));
        dataSource.setDriverClass(properties.getProperty("symqle.jdbc.driverClass"));
        dataSource.setUser(properties.getProperty("symqle.jdbc.user"));
        dataSource.setPassword(properties.getProperty("symqle.jdbc.password"));
        final String databaseName = DatabaseUtils.getDatabaseName(dataSource);
        String resource = databaseName + "DbSetup.sql";
        final Connection connection = dataSource.getConnection();
        initDatabase(connection, resource);
        connection.close();
        return dataSource;
    }


}
