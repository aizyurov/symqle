package org.symqle.integration.model;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.symqle.jdbc.CommonEngineFactory;
import org.symqle.jdbc.Engine;
import org.symqle.sql.Dialect;

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

    private final Engine engine;

    private ExternalDbEnvironment(final String configFile) {
        this.engine = createDialectDataSource(configFile);
    }

    private final static Map<String, ExternalDbEnvironment> instances = new HashMap<String, ExternalDbEnvironment>();

    public static ExternalDbEnvironment getInstance(String configFile) {
        synchronized (instances) {
            ExternalDbEnvironment environment = instances.get(configFile);
            if (environment == null) {
                environment = new ExternalDbEnvironment(configFile);
                instances.put(configFile, environment);
            }
            return environment;
        }
    }

    public Engine getEngine() {
        return engine;
    }

    public Engine createDialectDataSource(final String configFile) {
        try {
            final Properties properties = new Properties();
            final File propertiesFile = new File(configFile);
            properties.load(new FileInputStream(propertiesFile));
            final ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setJdbcUrl(properties.getProperty("symqle.jdbc.url"));
            dataSource.setDriverClass(properties.getProperty("symqle.jdbc.driverClass"));
            dataSource.setUser(properties.getProperty("symqle.jdbc.user"));
            dataSource.setPassword(properties.getProperty("symqle.jdbc.password"));
            final String dialectClass = System.getProperty("org.symqle.integration.dialect");
            if (dialectClass != null) {
                final Class<?> dialectClazz = Class.forName(dialectClass);
                final Method getMethod = dialectClazz.getMethod("get");
                final Dialect dialect = (Dialect) getMethod.invoke(null);
                return new CommonEngineFactory().create(dataSource, dialect);
            } else {
                return new CommonEngineFactory().create(dataSource);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
