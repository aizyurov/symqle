package org.symqle.coretest;

import junit.framework.TestCase;
import org.symqle.sql.GenericDialect;

/**
 * @author lvovich
 */
public class DialectTest extends TestCase {

    public void testGenericName() {
        assertEquals("Generic", GenericDialect.get().getName());
    }

}
