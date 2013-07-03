package org.symqle.sql;

import junit.framework.TestCase;
import org.symqle.querybuilder.ScalarNameProvider;

/**
 * @author lvovich
 */
public class ScalarNameProviderTest extends TestCase {

    public void testTwice() throws Exception {
        final ScalarNameProvider scalarNameProvider = new ScalarNameProvider();
        final String uniqueName = scalarNameProvider.getUniqueName();
        assertNotNull(uniqueName);
        try {
            final String uniqueName2 = scalarNameProvider.getUniqueName();
            fail("Expected IllegalStateException but returned " + uniqueName2);
        } catch (IllegalStateException e) {
            // fine
        }


    }
}
