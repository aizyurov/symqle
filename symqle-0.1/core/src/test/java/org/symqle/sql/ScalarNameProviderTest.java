package org.symqle.sql;

import junit.framework.TestCase;
import org.symqle.querybuilder.ScalarNameProvider;

/**
 * @author lvovich
 */
public class ScalarNameProviderTest extends TestCase {

    public void testName() throws Exception {
        final ScalarNameProvider scalarNameProvider = new ScalarNameProvider();
        final String uniqueName = scalarNameProvider.getUniqueName();
        assertEquals("C0", uniqueName);
    }
}
