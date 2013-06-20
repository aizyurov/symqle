package org.simqle.integration.model;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.simqle.front.DialectDataSource;
import org.simqle.sql.DatabaseGate;
import org.simqle.sql.Dialect;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author lvovich
 */
public class ExternalDbEnvironment implements TestEnvironment {

    private final DatabaseGate databaseGate;
    private final String databaseName;

    private ExternalDbEnvironment(final String databaseName) {
        this.databaseName = databaseName;
        this.databaseGate = createDialectDataSource(databaseName);
    }

    private final static Map<String, ExternalDbEnvironment> instances = new HashMap<String, ExternalDbEnvironment>();

    public static ExternalDbEnvironment getInstance(String databaseName) {
        synchronized (instances) {
            ExternalDbEnvironment environment = instances.get(databaseName);
            if (environment == null) {
                environment = new ExternalDbEnvironment(databaseName);
                instances.put(databaseName, environment);
            }
            return environment;
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DatabaseGate getGate() {
        return databaseGate;
    }

    public DatabaseGate createDialectDataSource(final String databaseName) {
        try {
            final Properties properties = new Properties();
            final File homeDir = new File(System.getProperty("user.home"));
            final File simqleSettingsDir = new File(homeDir, ".simqle");
            final File propertiesFile = new File(simqleSettingsDir, databaseName+".properties");
            properties.load(new FileInputStream(propertiesFile));
            final ComboPooledDataSource pool = new ComboPooledDataSource();
            pool.setJdbcUrl(properties.getProperty("simqle.jdbc.url"));
            pool.setDriverClass(properties.getProperty("simqle.jdbc.driverClass"));
            pool.setUser(properties.getProperty("simqle.jdbc.user"));
            pool.setPassword(properties.getProperty("simqle.jdbc.password"));
            final String dialectClass = properties.getProperty("simqle.jdbc.dialectClass");
            final String connectionSetup = properties.getProperty("simqle.jdbc.connectionSetup");
            final String effectiveClass = System.getProperty("org.simqle.integration.dialect", dialectClass);
            final Class<?> dialectClazz = Class.forName(effectiveClass);
            final Method getMethod = dialectClazz.getMethod("get");
            final Dialect dialect = (Dialect) getMethod.invoke(null);
            return new DialectDataSource(dialect, pool, connectionSetup);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
