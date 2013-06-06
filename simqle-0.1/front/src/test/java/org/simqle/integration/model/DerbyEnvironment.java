package org.simqle.integration.model;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.simqle.sql.Dialect;
import org.simqle.sql.DialectDataSource;

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
    private final String url = "jdbc:derby:memory:simqle";
    private final DialectDataSource dialectDataSource = createDialectDataSource();
    private final String databaseName = "derby";

    private DerbyEnvironment() {
    }

    private static DerbyEnvironment instance = new DerbyEnvironment();

    public static DerbyEnvironment getInstance() {
        return instance;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DialectDataSource createDialectDataSource() {
        try {
            final Connection connection = DriverManager.getConnection(url + ";create=true");
            initDatabase(connection);
            final ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setJdbcUrl(url);
            dataSource.setDriverClass(EmbeddedDriver.class.getName());
            final String effectiveClass = System.getProperty("org.simqle.integration.dialect", "org.simqle.derby.DerbyDialect");
            final Class<?> dialectClazz = Class.forName(effectiveClass);
            final Method getMethod = dialectClazz.getMethod("get");
            final Dialect dialect = (Dialect) getMethod.invoke(null);
            return new DialectDataSource(dialect, dataSource);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DialectDataSource getDialectDataSource() {
        return dialectDataSource;
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
