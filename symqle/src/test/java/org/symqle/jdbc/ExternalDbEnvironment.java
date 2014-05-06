package org.symqle.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.symqle.integration.model.AbstractTestEnvironment;
import org.symqle.jdbc.DatabaseUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lvovich
 */
public class ExternalDbEnvironment extends AbstractTestEnvironment {

    @Override
    public DataSource prepareDataSource(Properties properties, AtomicReference<String> userNameHolder) throws Exception {
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(properties.getProperty("symqle.jdbc.url"));
        dataSource.setDriverClass(properties.getProperty("symqle.jdbc.driverClass"));
        final String userName = properties.getProperty("symqle.jdbc.user");
        dataSource.setUser(userName);
        userNameHolder.set(userName);
        dataSource.setPassword(properties.getProperty("symqle.jdbc.password"));
        final String databaseName = DatabaseUtils.getDatabaseName(dataSource);
        String resource = databaseName + "DbSetup.sql";
        final Connection connection = dataSource.getConnection();
        initDatabase(connection, resource);
        connection.close();
        return dataSource;
    }


}
