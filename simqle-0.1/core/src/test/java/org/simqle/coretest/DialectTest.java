package org.simqle.coretest;

import junit.framework.TestCase;
import org.simqle.sql.GenericDialect;

/**
 * @author lvovich
 */
public class DialectTest extends TestCase {

    public void testGenericName() {
        assertEquals("Generic", GenericDialect.get().getName());
    }

}
