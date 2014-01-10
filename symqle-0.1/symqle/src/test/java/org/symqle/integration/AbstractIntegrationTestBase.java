package org.symqle.integration;

import junit.framework.TestCase;
import org.symqle.common.MalformedStatementException;
import org.symqle.integration.model.DerbyEnvironment;
import org.symqle.integration.model.ExternalDbEnvironment;
import org.symqle.integration.model.TestEnvironment;
import org.symqle.jdbc.Engine;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    private static TestEnvironment environment = createTestEnvironment();


    public Engine getEngine() {
        return environment.getEngine();
    }

    private static TestEnvironment createTestEnvironment() {
        final String database = System.getProperty("org.symqle.integration.config");
        return database == null ? DerbyEnvironment.getInstance() : ExternalDbEnvironment.getInstance(database);
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

    protected final void expectMalformedStatementException(MalformedStatementException e, Class... dialects) {
        if (Arrays.asList(dialects).contains(getEngine().getDialect().getClass())) {
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

}
