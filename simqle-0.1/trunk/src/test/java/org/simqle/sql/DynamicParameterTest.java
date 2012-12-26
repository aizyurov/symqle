package org.simqle.sql;

import org.simqle.Callback;
import org.simqle.Element;

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
            param.select().show();
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

    public void testWhere() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.where(id.eq(param)).show();
        assertSimilar("SELECT ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testInvalidIn() throws Exception {
        final LongColumn id = createId();
        final LongParameter param = new LongParameter(1L);
        final String sql;
        try {
            sql = id.in(param.select()).select().show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testAsFunctionArgument() throws Exception {
        final LongParameter param = new LongParameter(1L);
        final LongColumn id = createId();
        final String sql = new SqlFunction<Long>("abs") {
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

    public void testIn() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final LongColumn id2 = new LongColumn("id", employee);
        String sql = id.where(param.in(id2.select())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testNotInAll() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final LongColumn id2 = new LongColumn("id", employee);
        String sql = id.where(param.notIn(id2.select())).show();
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
        final String sql = param.select().orderBy(id).show();
        System.out.println(sql);
        assertSimilar("SELECT ? AS C1 FROM person AS T1 ORDER BY T1.id", sql);
    }

    public void testAsSortSpecification() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.select().orderBy(param).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ?", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.select().orderBy(param.nullsFirst()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.select().orderBy(param.nullsLast()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.select().orderBy(param.desc()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = id.select().orderBy(param.asc()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? ASC", sql);
    }

    public void testMult() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.mult(id).select().show();
        assertSimilar("SELECT ? * T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.mult(5).plus(id).select().show();
        assertSimilar("SELECT ? * ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        String sql = param.opposite().where(id.booleanValue()).show();
        assertSimilar("SELECT - ? AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testPlus() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.plus(id).select().show();
        assertSimilar("SELECT ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.plus(2).plus(id).select().show();
        assertSimilar("SELECT ? + ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.minus(id).select().show();
        assertSimilar("SELECT ? - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.minus(1.3).plus(id).select().show();
        assertSimilar("SELECT ? - ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.div(id).select().show();
        assertSimilar("SELECT ? / T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.div(4L).plus(id).select().show();
        assertSimilar("SELECT ? / ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.concat(id).select().show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testString() throws Exception {
        final LongColumn id  =  createId();
        final LongParameter param = new LongParameter(1L);
        final String sql = param.concat(" ").concat(id).select().show();
        assertSimilar("SELECT ? || ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testList() throws Exception {
        final DataSource dataSource = org.easymock.EasyMock.createMock(DataSource.class);
        final LongParameter param = new LongParameter(1L);
        // dataSource should never be called
        org.easymock.EasyMock.replay(dataSource);
        try {
            param.select().list(dataSource);
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
            param.select().scroll(dataSource, new Callback<Long, SQLException>() {
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
