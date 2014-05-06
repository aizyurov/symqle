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
public class HsqlEnvironment extends AbstractTestEnvironment {
    private final String url = "jdbc:hsqldb:mem:mymemdb";

    public DataSource prepareDataSource(final Properties properties, final AtomicReference<String> userNameHolder) throws Exception {
        final Connection connection = DriverManager.getConnection(url, "SA", "");
        initDatabase(connection, "hsqlDbSetup.sql");
        connection.close();
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setDriverClass(EmbeddedDriver.class.getName());
        userNameHolder.set("SA");
        return dataSource;
    }


}
