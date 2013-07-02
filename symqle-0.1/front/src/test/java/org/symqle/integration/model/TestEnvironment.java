package org.symqle.integration.model;

import org.symqle.sql.DatabaseGate;

/**
 * @author lvovich
 */
public interface TestEnvironment {
    DatabaseGate getGate();
}
