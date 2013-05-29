package org.simqle.integration.model;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.simqle.sql.Dialect;
import org.simqle.sql.DialectDataSource;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * @author lvovich
 */
public class ExternalDbEnvironment implements TestEnvironment {

    private DialectDataSource dialectDataSource;
    private String databaseName;

    public DialectDataSource getDialectDataSource() {
        return dialectDataSource;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public void doSetUp(final String databaseName) throws Exception {
        this.databaseName = databaseName;
        final Properties properties = new Properties();
        final File homeDir = new File(System.getProperty("user.home"));
        final File simqleSettingsDir = new File(homeDir, ".simqle");
        final File propertiesFile = new File(simqleSettingsDir, databaseName+".properties");
        properties.load(new FileInputStream(propertiesFile));
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(properties.getProperty("simqle.jdbc.url"));
        dataSource.setDriverClass(properties.getProperty("simqle.jdbc.driverClass"));
        dataSource.setUser(properties.getProperty("simqle.jdbc.user"));
        dataSource.setPassword(properties.getProperty("simqle.jdbc.password"));
        final String dialectClass = properties.getProperty("simqle.jdbc.dialectClass");
        final String effectiveClass = dialectClass !=null ? dialectClass : "org.simqle.sql.GenericDialect";
        final Class<?> dialectClazz = Class.forName(effectiveClass);
        final Method getMethod = dialectClazz.getMethod("get");
        final Dialect dialect = (Dialect) getMethod.invoke(null);
        dialectDataSource = new DialectDataSource(dialect, dataSource);
    }

    @Override
    public void doTearDown() throws Exception {
        ((ComboPooledDataSource) dialectDataSource.getDataSource()).close();
        dialectDataSource = null;
        databaseName = null;
    }
}
