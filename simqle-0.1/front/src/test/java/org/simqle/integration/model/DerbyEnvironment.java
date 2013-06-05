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
import java.sql.SQLException;

/**
 * @author lvovich
 */
public class DerbyEnvironment implements TestEnvironment {
    private String url;
    private DialectDataSource dialectDataSource;
    private final String databaseName = "derby";
    private ComboPooledDataSource dataSource;

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public void doSetUp(final String testName) throws Exception {
        url = "jdbc:derby:memory:"+testName;
        final Connection connection = DriverManager.getConnection(url + ";create=true");
        initDatabase(connection);
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setDriverClass(EmbeddedDriver.class.getName());
        final String effectiveClass = System.getProperty("org.simqle.integration.dialect", "org.simqle.sql.GenericDialect");
        final Class<?> dialectClazz = Class.forName(effectiveClass);
        final Method getMethod = dialectClazz.getMethod("get");
        final Dialect dialect = (Dialect) getMethod.invoke(null);
        dialectDataSource = new DialectDataSource(dialect, dataSource);
    }

    @Override
    public void doTearDown() throws Exception {
        dataSource.close();
        try {
            try {
               DriverManager.getConnection(url+";drop=true");
            } catch (SQLException se)  {
                final String sqlState = se.getSQLState();
                if ( sqlState.equals("08006") ) {
                  return;
               } else {
                    throw se;
               }
            }
            throw new IllegalStateException("Database did not shut down normally");
        } finally {
            url = null;
            dialectDataSource = null;
        }
    }

    @Override
    public DialectDataSource getDialectDataSource() {
        return dialectDataSource;
    }

    private void initDatabase(final Connection connection) throws Exception {
    final BufferedReader reader = new BufferedReader(
            new InputStreamReader(getClass().getClassLoader().getResourceAsStream("defaultDbSetup.sql")));
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
