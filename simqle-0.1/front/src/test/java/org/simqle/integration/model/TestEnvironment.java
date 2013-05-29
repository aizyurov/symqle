package org.simqle.integration.model;

import org.simqle.sql.DialectDataSource;

/**
 * @author lvovich
 */
public interface TestEnvironment {
    void doSetUp(String testName) throws Exception;
    void doTearDown() throws Exception;
    DialectDataSource getDialectDataSource();
    String getDatabaseName();
}
