package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.JoinTestTable;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class JoinTest extends AbstractIntegrationTestBase {

    public void testLeftJoin() throws Exception {
        final JoinTestTable left = new JoinTestTable() {
            @Override
            public String getTableName() {
                return "join_test_left";
            }
        };
        final JoinTestTable right = new JoinTestTable() {
            @Override
            public String getTableName() {
                return "join_test_right";
            }
        };
        left.leftJoin(right, left.id().eq(right.id()));
        final List<Pair<String,String>> list = left.text().pair(right.text()).orderBy(left.text()).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("one", (String) null), Pair.make("two", "two")), list);
    }


    public void testRightJoin() throws Exception {
        final JoinTestTable left = new JoinTestTable() {
            @Override
            public String getTableName() {
                return "join_test_left";
            }
        };
        final JoinTestTable right = new JoinTestTable() {
            @Override
            public String getTableName() {
                return "join_test_right";
            }
        };
        left.rightJoin(right, left.id().eq(right.id()));
        final List<Pair<String,String>> list = left.text().pair(right.text()).orderBy(right.text()).list(getEngine());
        assertEquals(Arrays.asList(Pair.make((String) null, "three"), Pair.make("two", "two")), list);
    }

    public void testInnerJoin() throws Exception {
        final JoinTestTable left = new JoinTestTable() {
            @Override
            public String getTableName() {
                return "join_test_left";
            }
        };
        final JoinTestTable right = new JoinTestTable() {
            @Override
            public String getTableName() {
                return "join_test_right";
            }
        };
        left.innerJoin(right, left.id().eq(right.id()));
        final List<Pair<String,String>> list = left.text().pair(right.text()).orderBy(right.text()).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("two", "two")), list);
    }

    public void testOuterJoin() throws Exception {
        final JoinTestTable left = new JoinTestTable() {
            @Override
            public String getTableName() {
                return "join_test_left";
            }
        };
        final JoinTestTable right = new JoinTestTable() {
            @Override
            public String getTableName() {
                return "join_test_right";
            }
        };
        left.outerJoin(right, left.id().eq(right.id()));
        try {
            final List<Pair<String,String>> list = left.text().pair(right.text()).orderBy(right.text().nullsLast()).list(getEngine());
            assertEquals(Arrays.asList(Pair.make((String) null, "three"), Pair.make("two", "two"), Pair.make("one", (String)null)), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "OUTER" at line 1, column 63.
            // mysql: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'OUTER JOIN
            // org.postgresql.util.PSQLException: ERROR: syntax error at or near "OUTER"
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

}
