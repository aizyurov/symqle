package org.symqle.integration.model;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.derby.jdbc.EmbeddedDriver;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lvovich
 */
public class DerbyEnvironment extends AbstractTestEnvironment {
    private final String url = "jdbc:derby:memory:symqle";

    public DataSource prepareDataSource(final Properties properties, final AtomicReference<String> userNameHolder) throws Exception {
        final Connection connection = DriverManager.getConnection(url + ";create=true");
        initDatabase(connection, "defaultDbSetup.sql");
        connection.close();
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setDriverClass(EmbeddedDriver.class.getName());
        userNameHolder.set("APP");
        return dataSource;
    }


}
