package org.symqle;

import junit.framework.TestCase;
import org.symqle.common.SqlContext;

/**
 * @author lvovich
 */
public class SqlContextTest extends TestCase {

    public void testPutGet() throws Exception {
        final SqlContext.Builder builder = new SqlContext.Builder();
        final SqlContext context = builder.put(String.class, "abc").toSqlContext();
        assertEquals("abc", context.get(String.class));

    }
}
