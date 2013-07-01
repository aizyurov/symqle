package org.symqle.integration.model;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.symqle.front.DialectDataSource;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.Dialect;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author lvovich
 */
public class DerbyEnvironment implements TestEnvironment {
    private final String url = "jdbc:derby:memory:symqle";
    private final DatabaseGate gate = createDatabaseGate();

    private DerbyEnvironment() {
    }

    private static DerbyEnvironment instance = new DerbyEnvironment();

    public static DerbyEnvironment getInstance() {
        return instance;
    }

    public DatabaseGate createDatabaseGate() {
        try {
            final Connection connection = DriverManager.getConnection(url + ";create=true");
            initDatabase(connection);
            final ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setJdbcUrl(url);
            dataSource.setDriverClass(EmbeddedDriver.class.getName());
            final String dialectClass = System.getProperty("org.symqle.integration.dialect");
            if (dialectClass != null) {
                final Class<?> dialectClazz = Class.forName(dialectClass);
                final Method getMethod = dialectClazz.getMethod("get");
                final Dialect dialect = (Dialect) getMethod.invoke(null);
                return new DialectDataSource(dataSource, dialect);
            } else {
                return new DialectDataSource(dataSource);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DatabaseGate getGate() {
        return gate;
    }

    private void initDatabase(final Connection connection) throws Exception {
    final BufferedReader reader = new BufferedReader(
            new InputStreamReader(DerbyEnvironment.class.getClassLoader().getResourceAsStream("defaultDbSetup.sql")));
    try {
        final StringBuilder builder = new StringBuilder();
        try {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.equals("")) {
                    final String sql = builder.toString();
                    builder.setLength(0);
                    if (sql.trim().length()>0) {
//                        System.out.println(sql);
                        final PreparedStatement preparedStatement = connection.prepareStatement(sql);
                        preparedStatement.executeUpdate();
                        preparedStatement.close();
                    }
                } else {
                    builder.append(" ").append(line);
                }
            }
        } finally {
            connection.close();
        }
    } finally {
        reader.close();
    }
    }

}
