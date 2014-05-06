package org.symqle.gate;

import org.symqle.integration.AbstractIntegrationTestBase;

/**
 * @author lvovich
 */
public class PsqlConnectionDebug extends AbstractIntegrationTestBase {

    public void test() throws Exception {
        final String databaseName = getEngine().getDatabaseName();
        System.out.println(databaseName);
    }
}
