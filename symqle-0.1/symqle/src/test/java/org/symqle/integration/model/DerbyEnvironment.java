package org.symqle.integration.model;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.symqle.jdbc.ConnectorEngine;
import org.symqle.jdbc.Engine;
import org.symqle.sql.Dialect;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author lvovich
 */
public class DerbyEnvironment extends AbstractTestEnvironment {
    private final String url = "jdbc:derby:memory:symqle";
    private final Engine engine = createEngine();

    private DerbyEnvironment() {
    }

    private static DerbyEnvironment instance = new DerbyEnvironment();

    public static DerbyEnvironment getInstance() {
        return instance;
    }

    public Engine createEngine() {
        try {
            final Connection connection = DriverManager.getConnection(url + ";create=true");
            initDatabase(connection, "defaultDbSetup.sql");
            connection.close();
            final ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setJdbcUrl(url);
            dataSource.setDriverClass(EmbeddedDriver.class.getName());
            final String dialectClass = System.getProperty("org.symqle.integration.dialect");
            if (dialectClass != null) {
                final Class<?> dialectClazz = Class.forName(dialectClass);
                final Method getMethod = dialectClazz.getMethod("get");
                final Dialect dialect = (Dialect) getMethod.invoke(null);
                return new ConnectorEngine(dataSource, dialect);
            } else {
                return new ConnectorEngine(dataSource);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Engine getEngine() {
        return engine;
    }

}
