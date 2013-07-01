package org.simqle.integration.model;

import org.simqle.sql.DatabaseGate;

/**
 * @author lvovich
 */
public interface TestEnvironment {
    DatabaseGate getGate();
}
