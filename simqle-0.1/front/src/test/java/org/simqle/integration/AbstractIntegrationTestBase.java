package org.simqle.integration;

import junit.framework.TestCase;
import org.simqle.integration.model.DerbyEnvironment;
import org.simqle.integration.model.ExternalDbEnvironment;
import org.simqle.integration.model.TestEnvironment;
import org.simqle.sql.DialectDataSource;

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

    private TestEnvironment environment;


    public DialectDataSource getDialectDataSource() {
        return environment.getDialectDataSource();
    }

    @Override
    protected final void setUp() throws Exception {
        final String database = System.getProperty("org.simqle.integration.database");
        if (database == null) {
            environment = new DerbyEnvironment();
            environment.doSetUp(getClass().getName() + "." + getName());
       } else {
            environment = new ExternalDbEnvironment();
            environment.doSetUp(database);
        }
    }

    protected void onSetUp() throws Exception {

    }

    protected final String getDatabaseName() {
        return environment.getDatabaseName();
    }

    @Override
    protected void tearDown() throws Exception {
        environment.doTearDown();
    }

}
