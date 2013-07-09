package org.symqle;

import junit.framework.TestCase;
import org.symqle.common.NiceSql;
import org.symqle.common.SqlParameters;

import java.sql.SQLException;

/**
 * @author lvovich
 */
public class NiceSqlTest extends TestCase {

    private static class PlainSql extends NiceSql {
        private final String text;

        private PlainSql(final String text) {
            this.text = text;
        }

        @Override
        public String sql() {
            return text;
        }

        @Override
        public void setParameters(final SqlParameters p) throws SQLException {
            // do nothing
        }
    }

    public void testSpaces() throws Exception {
        final PlainSql sql = new PlainSql(" SELECT ABS ( T . a ) AS a , T . b AS b FROM s AS T WHERE a      =  b");
        assertEquals("SELECT ABS(T.a) AS a, T.b AS b FROM s AS T WHERE a = b", sql.toString());
    }
}
