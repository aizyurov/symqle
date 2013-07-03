package org.symqle.jdbctest;

import junit.framework.TestCase;
import org.symqle.jdbc.UpdatableConfiguration;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 29.06.2013
 * Time: 0:30:46
 * To change this template use File | Settings | File Templates.
 */
public class ConfigurationTest extends TestCase {

    // this values are documented. If defaults change, correct the documentation
    public void testDefaults() throws Exception {
        UpdatableConfiguration configuration  = new UpdatableConfiguration();
        assertFalse(configuration.allowImplicitCrossJoins());
        assertFalse(configuration.allowNoFrom());
    }

    public void testSet() throws Exception {
        UpdatableConfiguration configuration  = new UpdatableConfiguration();
        assertFalse(configuration.allowImplicitCrossJoins());
        assertFalse(configuration.allowNoFrom());
        configuration.setImplicitCrossJoinsOk(true);
        assertTrue(configuration.allowImplicitCrossJoins());
        configuration.setNoFromOk(true);
        assertTrue(configuration.allowNoFrom());


    }
}
