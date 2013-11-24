package org.symqle.sql;

import junit.framework.TestCase;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 24.11.2013
 * Time: 11:42:51
 * To change this template use File | Settings | File Templates.
 */
public class TableRegistryTest extends TestCase {

    public void testDataChangeRegistryNotSupportsGetLocal() {
        try {
            new DataChangeTableRegistry().getLocal();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("symqle.org"));
        }
    }
}
