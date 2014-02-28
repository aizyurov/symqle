package org.symqle;

import junit.framework.TestCase;
import org.symqle.querybuilder.CustomSql;
import org.symqle.querybuilder.SqlFormatter;

/**
 * @author lvovich
 */
public class SqlFormatterTest extends TestCase {

    public void testTrivial() {
        assertEquals("abcd", SqlFormatter.formatText(new CustomSql("abcd")));
    }

    public void testEmpty() {
        assertEquals("", SqlFormatter.formatText(new CustomSql("")));
    }

    public void testSpaces() {
        assertEquals("select abs(t.a), t.b from my_table t", SqlFormatter.formatText(new CustomSql("select abs  (  t  . a ) , t . b from my_table t")));
    }

    public void testIdempotent() {
        final String first = SqlFormatter.formatText(new CustomSql("select abs  (  t  . a ) , t . b from my_table t"));
        final String second = SqlFormatter.formatText(new CustomSql(first));
        assertEquals(second, first);

    }
}
