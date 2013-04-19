package org.simqle.integration;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import junit.framework.TestCase;
import org.hsqldb.jdbcDriver;
import org.simqle.sql.Dialect;
import org.simqle.sql.DialectDataSource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Base class for integration tests.
 * DataSource is initialzied from System.properties to allow
 * testing against different RDBMS.
 * The following properties must be defined:
 * <ul>
 *     <li>simqle.jdbc.url</li>
 *     <li>simqle.jdbc.user</li>
 *     <li>simqle.jdbc.password</li>
 *     <li>simqle.jdbc.driverClass (fqn)</li>
 *     <li>simqle.jdbc.dialectClass (fqn)</li>
 *     <li>simqle.jdbc.dbSetup</li>
 * </ul>
 * Dialect class should have public static Dialect get() method.
 * DumpResource is a location of a resource (no the classpath)
 * @author lvovich
 */
public abstract class AbstractIntegrationTestBase extends TestCase {

    private final DialectDataSource dialectDataSource;

    private boolean databaseInitialized;

    protected AbstractIntegrationTestBase() throws Exception {
        final String url = System.getProperty("simqle.jdbc.url");
        final String user = System.getProperty("simqle.jdbc.user");
        final String password = System.getProperty("simqle.jdbc.password");
        final String driverClass = System.getProperty("simqle.jdbc.driverClass");

        final String effectiveUrl = url !=null ? url : "jdbc:hsqldb:mem:simqle";
        final String effectiveUser = url !=null ? user : "SA";
        final String effectivePassword = url !=null ? password : "";
        final String effectiveDriverClass = url !=null ? driverClass : jdbcDriver.class.getName();

        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(effectiveUrl);
        dataSource.setDriverClass(effectiveDriverClass);
        dataSource.setUser(effectiveUser);
        dataSource.setPassword(effectivePassword);

        final String dialectClass = System.getProperty("simqle.jdbc.dialectClass");
        final String effectiveClass = url !=null ? dialectClass : "org.simqle.sql.GenericDialect";
        final Class<?> dialectClazz = Class.forName(effectiveClass);
        final Method getMethod = dialectClazz.getMethod("get");
        final Dialect dialect = (Dialect) getMethod.invoke(null);

        dialectDataSource = new DialectDataSource(dialect, dataSource);
    }

    public DialectDataSource getDialectDataSource() {
        return dialectDataSource;
    }

    @Override
    protected final void setUp() throws Exception {
        if (!databaseInitialized) {
            initDatabase();
            databaseInitialized = true;
        }
        onSetUp();
    }

    protected void onSetUp() throws Exception {

    }

    private void initDatabase() throws Exception {
        final DataSource dataSource = dialectDataSource.getDataSource();
        final String dbSetupResource = System.getProperty("simqle.jdbc.dbSetup");
        final String effectiveResource = dbSetupResource != null ? dbSetupResource : "defaultDbSetup.sql";
        final BufferedReader reader = new BufferedReader(
                new InputStreamReader(getClass().getClassLoader().getResourceAsStream(effectiveResource)));
        try {
            final Connection connection = dataSource.getConnection();
            final StringBuilder builder = new StringBuilder();
            try {
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    if (line.equals("")) {
                        final String sql = builder.toString();
                        builder.setLength(0);
                        if (sql.trim().length()>0) {
                            System.out.println(sql);
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
