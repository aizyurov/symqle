package org.simqle.integration;

import junit.framework.TestCase;
import org.simqle.integration.model.DerbyEnvironment;
import org.simqle.integration.model.ExternalDbEnvironment;
import org.simqle.integration.model.TestEnvironment;
import org.simqle.sql.DatabaseGate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for integration tests.
 * dialectDataSource is initialized depending on system property
 * "org.simqle.integration.database". The value is used according to
 * the following naming convention:
 * it is expected a file "${user.home}/.simqle/${org.simqle.integration.database}.properties", which
 * contains simqle.jdbc.url, simqle.jdbc.user, simqle.jdbc.password, simqle.jdbc.driverClass and simqle.jdbc.dialectClass
 * parameters. Is is expected that the schema already contains all necessary data.
 * If org.simqle.integration.database is undefined, the dialect is GenericDialect,
 * the database id derby embedded database; a schema is created before each test, filled from defaultDbSetup.sql
 * and dropped after the test.
 * @author lvovich
 */
public abstract class AbstractIntegrationTestBase extends TestCase {

    private static TestEnvironment environment = createTestEnvironment();


    public DatabaseGate getDatabaseGate() {
        return environment.getGate();
    }

    private static TestEnvironment createTestEnvironment() {
        final String database = System.getProperty("org.simqle.integration.config");
        return database == null ? DerbyEnvironment.getInstance() : ExternalDbEnvironment.getInstance(database);
    }

    @Override
    protected final void setUp() throws Exception {
        onSetUp();
    }

    protected void onSetUp() throws Exception {

    }

    protected final void expectSQLException(SQLException e, String... databaseNames) throws SQLException {
        if (Arrays.asList(databaseNames).contains(environment.getGate().getDialect().getName())) {
            return;
        }
        throw e;
    }

    protected final void expectIllegalStateException(IllegalStateException e, Class... dialects) {
        if (Arrays.asList(dialects).contains(getDatabaseGate().getDialect().getClass())) {
            return;
        }
        throw e;
    }

    protected final String getDatabaseName() {
        return environment.getGate().getDialect().getName();
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

}
