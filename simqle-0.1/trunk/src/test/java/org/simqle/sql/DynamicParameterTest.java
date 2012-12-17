package org.simqle.sql;

import org.simqle.Callback;
import org.simqle.Element;
import org.simqle.Function;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 21.11.12
 * Time: 20:50
 * To change this template use File | Settings | File Templates.
 */
public class DynamicParameterTest extends SqlTestCase {


    public void testSelectStatementNoFrom() throws Exception {
        final LongParameter param = new LongParameter(1L);
        try {
            param.show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    private LongColumn createId() {
        return new LongColumn("id", person);
    }

    public void testSelectAll() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.all().where(id.eq(param)).show();
        assertSimilar("SELECT ALL ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testSelectDistinct() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.distinct().where(id.eq(param)).show();
        assertSimilar("SELECT DISTINCT ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testSelect() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.where(id.eq(param)).show();
        assertSimilar("SELECT ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testAsFunctionArgument() throws Exception {
        final LongParameter param = new LongParameter(1L);
        final LongColumn id = createId();
        final String sql = new FunctionCall<Long>("abs") {
            @Override
            public Long value(final Element element) throws SQLException {
                return element.getLong();
            }
        }.apply(param).where(id.booleanValue()).show();
        assertSimilar("SELECT abs(?) AS C1 FROM person AS T1 WHERE T1.id", sql);
    }

    public void testAsCondition() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.where(param.booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ?", sql);
    }

    public void testEq() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.where(param.eq(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? = T0.id", sql);
    }

    public void testNe() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.where(param.ne(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <> T0.id", sql);
    }

    public void testGt() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.where(param.gt(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? > T0.id", sql);
    }

    public void testGe() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.where(param.ge(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? >= T0.id", sql);
    }

    public void testLt() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.where(param.lt(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? < T0.id", sql);
    }

    public void testLe() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.where(param.le(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <= T0.id", sql);
    }

    public void testExceptAll() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.exceptAll(id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testExceptDistinct() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.exceptDistinct(id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testExcept() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.except(id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testUnionAll() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.unionAll(id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testUnionDistinct() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.unionDistinct(id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testUnion() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.union(id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testIntersectAll() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.intersectAll(id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testIntersectDistinct() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.intersectDistinct(id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testIntersect() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.intersect(id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testSelectForUpdate() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.forUpdate().show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testSelectForReadOnly() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            param.forReadOnly().show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testExists() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        try {
            id.where(param.exists()).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testIn() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final LongColumn id2 = new LongColumn("id", employee);
        String sql = id.where(param.in(id2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testNotInAll() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final LongColumn id2 = new LongColumn("id", employee);
        String sql = id.where(param.notIn(id2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old

        final LongParameter param = new LongParameter(1L);
        final ValueExpression<Long> expr = new LongParameter(1L);
        final ValueExpression<Long> expr2 = new LongParameter(2L);
        final ValueExpression<Long> expr3 = new LongParameter(3L);
        String sql = id.where(param.in(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);

        final ValueExpression<Long> expr = new LongParameter(1L);
        final ValueExpression<Long> expr2 = new LongParameter(2L);
        final ValueExpression<Long> expr3 = new LongParameter(3L);
        String sql = id.where(param.notIn(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        String sql = id.where(param.isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        String sql = id.where(param.isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.orderBy(id).show();
        System.out.println(sql);
        assertSimilar("SELECT ? AS C1 FROM person AS T1 ORDER BY T1.id", sql);
    }

    public void testAsSortSpecification() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.orderBy(param).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ?", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.orderBy(param.nullsFirst()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.orderBy(param.nullsLast()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.orderBy(param.desc()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.orderBy(param.asc()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? ASC", sql);
    }

    public void testMult() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.mult(id).show();
        assertSimilar("SELECT ? * T0.id AS C0 FROM person AS T0", sql);
    }

    public void testPlus() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.plus(id).show();
        assertSimilar("SELECT ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.minus(id).show();
        assertSimilar("SELECT ? - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.div(id).show();
        assertSimilar("SELECT ? / T0.id AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.concat(id).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final Person person = new Person();
        final LongColumn id = new LongColumn("id", person);
        final LongParameter param = new LongParameter(1L);
        assertSimilar("SELECT ? AS C1, T1.id AS C2 FROM person AS T1", param.pair(id).show());
    }

    public void testConvert() throws Exception {
        final Person person = new Person();
        final LongColumn id = new LongColumn("id", person);
        final LongParameter param = new LongParameter(1L);
        final String sql = param.convert(new Function<Long, String>() {
            @Override
            public String apply(final Long arg) {
                return String.valueOf(arg);
            }
        }).pair(id).show();
        assertSimilar("SELECT ? AS C0, T1.id AS C1 FROM person AS T1", sql);

    }

    public void testList() throws Exception {
        final DataSource dataSource = org.easymock.EasyMock.createMock(DataSource.class);
        final LongParameter param = new LongParameter(1L);
        // dataSource should never be called
        org.easymock.EasyMock.replay(dataSource);
        try {
            param.list(dataSource);
            fail ("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
        org.easymock.EasyMock.verify(dataSource);
    }

    public void testScroll() throws Exception {
        final DataSource dataSource = org.easymock.EasyMock.createMock(DataSource.class);
        final LongParameter param = new LongParameter(1L);
        // dataSource should never be called
        org.easymock.EasyMock.replay(dataSource);
        try {
            param.scroll(dataSource, new Callback<Long, SQLException>() {
                @Override
                public void iterate(final Long aLong) throws SQLException, BreakException {
                    fail("Must not get here");
                }
            });
            fail ("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
        org.easymock.EasyMock.verify(dataSource);
    }


    private static class Person extends Table {
        private Person() {
            super("person");
        }
    }

    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
    }

    private static Person person = new Person();

    private static Employee employee = new Employee();

}
