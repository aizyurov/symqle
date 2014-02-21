package org.symqle.integration;

import junit.framework.TestCase;
import org.symqle.integration.model.DerbyEnvironment;
import org.symqle.integration.model.ExternalDbEnvironment;
import org.symqle.integration.model.TestEnvironment;
import org.symqle.jdbc.ConnectorEngine;
import org.symqle.jdbc.Engine;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Base class for integration tests.
 * dialectDataSource is initialized depending on system property
 * "org.symqle.integration.database". The value is used according to
 * the following naming convention:
 * it is expected a file "${user.home}/.symqle/${org.symqle.integration.database}.properties", which
 * contains symqle.jdbc.url, symqle.jdbc.user, symqle.jdbc.password, symqle.jdbc.driverClass and symqle.jdbc.dialectClass
 * parameters. Is is expected that the schema already contains all necessary data.
 * If org.symqle.integration.database is undefined, the dialect is GenericDialect,
 * the database id derby embedded database; a schema is created before each test, filled from defaultDbSetup.sql
 * and dropped after the test.
 * @author lvovich
 */
public abstract class AbstractIntegrationTestBase extends TestCase {

    private static DataSource dataSource;


    public final Engine getEngine() {
        try {
            if (dataSource == null) {
                dataSource = prepareDataSource();
            }
            return createTestEngine(dataSource);
        } catch (Exception e) {
            throw new RuntimeException("Internal error", e);
        }
    }

    protected Engine createTestEngine(final DataSource dataSource) throws SQLException {
        return new ConnectorEngine(dataSource);
    }

    protected final DataSource prepareDataSource() throws Exception {
        final String config = System.getProperty("org.symqle.integration.config");
        if (config == null) {
            return new DerbyEnvironment().prepareDataSource(new Properties());
        } else {
            Properties properties = new Properties();
            final File propertiesFile = new File(config);
            properties.load(new FileInputStream(propertiesFile));
            final String environment = properties.getProperty("org.symqle.integration.environment");
            if (environment == null) {
                return new ExternalDbEnvironment().prepareDataSource(properties);
            } else {
                try {
                    final TestEnvironment testEnvironment = (TestEnvironment) Class.forName(environment).newInstance();
                    return testEnvironment.prepareDataSource(properties);
                } catch (InstantiationException e) {
                    throw new RuntimeException("Misconfiguration error", e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Misconfiguration error", e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Misconfiguration error", e);
                }
            }
        }
    }

    @Override
    protected final void setUp() throws Exception {
        onSetUp();
    }

    protected void onSetUp() throws Exception {

    }

    protected final void expectSQLException(SQLException e, String... databaseNames) throws SQLException {
        if (Arrays.asList(databaseNames).contains(getDatabaseName())) {
            return;
        }
        throw e;
    }

    protected final String getDatabaseName() {
        return getEngine().getDatabaseName();
    }

    public static List<Double> toListOfDouble(final List<Number> list, Double nullReplacement) {
        final List<Double> doubles = new ArrayList<Double>();
        for (Number n: list) {
            doubles.add(n == null ? nullReplacement : n.doubleValue());
        }
        return doubles;
    }

    public static List<Double> toListOfDouble(final List<Number> list) {
        return toListOfDouble(list, null);
    }

    protected String validCollationNameForVarchar() {
        final String collationName;
        final String databaseName = getEngine().getDatabaseName();
        if (databaseName.equals("MySQL")) {
            collationName = "utf8_unicode_ci";
        } else if (databaseName.equals("PostgreSQL")) {
            collationName = "\"en_US.utf8\"";
        } else {
            collationName = "default";
        }
        return collationName;
    }

    protected String validCollationNameForChar() {
        final String collationName;
        final String databaseName = getEngine().getDatabaseName();
        if (databaseName.equals("MySQL")) {
            collationName = "utf8mb4_unicode_ci";
        } else if (databaseName.equals("PostgreSQL")) {
            collationName = "\"en_US.utf8\"";
        } else {
            collationName = "default";
        }
        return collationName;
    }

    protected String validCollationNameForNumber() {
        final String collationName;
        final String databaseName = getEngine().getDatabaseName();
        if (databaseName.equals("MySQL")) {
            collationName = "latin1_general_ci";
        } else if (databaseName.equals("PostgreSQL")) {
            collationName = "\"en_US\"";
        } else {
            collationName = "default";
        }
        return collationName;
}

}
