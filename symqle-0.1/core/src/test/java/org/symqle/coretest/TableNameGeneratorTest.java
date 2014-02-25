package org.symqle.coretest;

import junit.framework.TestCase;
import org.symqle.querybuilder.TableNameGenerator;

/**
 * @author lvovich
 */
public class TableNameGeneratorTest extends TestCase {

    public void testLongPrefix() {
        final String prefix = "abcdefghijklmnopqrstuvwxyz";
        final String generated = new TableNameGenerator().generate(prefix);
        assertEquals(21, generated.length());
    }
}
