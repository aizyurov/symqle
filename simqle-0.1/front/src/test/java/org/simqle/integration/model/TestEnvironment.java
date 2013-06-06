package org.simqle.integration.model;

import org.simqle.sql.DialectDataSource;

/**
 * @author lvovich
 */
public interface TestEnvironment {
    DialectDataSource getDialectDataSource();
    String getDatabaseName();
}
