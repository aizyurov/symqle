package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.ElementMapper;
import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.SqlFunction;
import org.simqle.sql.TableOrView;
import org.simqle.sql.ValueExpression;

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
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testSelectAll() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.all().where(id.eq(param)).show();
        assertSimilar("SELECT ALL ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testSelectDistinct() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.distinct().where(id.eq(param)).show();
        assertSimilar("SELECT DISTINCT ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.pair(id).show();
        assertSimilar("SELECT ? AS C1, T1.id AS C2 FROM person AS T1", sql);
    }

    public void testWhere() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.where(id.eq(param)).show();
        assertSimilar("SELECT ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testAsFunctionArgument() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final Column<Long> id = person.id;
        final String sql = new SqlFunction<Long>("abs") {
            @Override
            public ElementMapper<Long> getElementMapper() {
                return Mappers.LONG;
            }
        }.apply(param).where(id.booleanValue()).show();
        assertSimilar("SELECT abs(?) AS C1 FROM person AS T1 WHERE T1.id", sql);
    }

    public void testAsCondition() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ?", sql);
    }

    public void testEq() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.eq(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? = T0.id", sql);
    }

    public void testNe() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.ne(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <> T0.id", sql);
    }

    public void testGt() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.gt(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? > T0.id", sql);
    }

    public void testGe() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.ge(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? >= T0.id", sql);
    }

    public void testLt() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.lt(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? < T0.id", sql);
    }

    public void testLe() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.le(id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <= T0.id", sql);
    }

    public void testEqValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.eq(2L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? = ?", sql);
    }


    public void testNeValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.ne(2L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <> ?", sql);
    }


    public void testLtValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.lt(2L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? < ?", sql);
    }


    public void testLeValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.le(2L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <= ?", sql);
    }


    public void testGtValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.gt(2L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? > ?", sql);
    }


    public void testGeValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.ge(2L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? >= ?", sql);
    }

    public void testAsInSubquery() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            String sql = id.where(id.in(param)).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("does not support"));
        }
    }



    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final Column<Long> id2 = employee.id;
        String sql = id.where(param.in(id2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testNotInAll() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final Column<Long> id2 = employee.id;
        String sql = id.where(param.notIn(id2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpression<Long> expr = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpression<Long> expr2 = DynamicParameter.create(Mappers.LONG, 2L);
        final ValueExpression<Long> expr3 = DynamicParameter.create(Mappers.LONG, 3L);
        String sql = id.where(param.in(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);

        final ValueExpression<Long> expr = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpression<Long> expr2 = DynamicParameter.create(Mappers.LONG, 2L);
        final ValueExpression<Long> expr3 = DynamicParameter.create(Mappers.LONG, 3L);
        String sql = id.where(param.notIn(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        String sql = id.where(param.isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        String sql = id.where(param.isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.orderBy(id).show();
        System.out.println(sql);
        assertSimilar("SELECT ? AS C1 FROM person AS T1 ORDER BY T1.id", sql);
    }

    public void testAsSortSpecification() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.orderBy(param).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ?", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.orderBy(param.nullsFirst()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.orderBy(param.nullsLast()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.orderBy(param.desc()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.orderBy(param.asc()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? ASC", sql);
    }

    public void testMult() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.mult(id).show();
        assertSimilar("SELECT ? * T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.mult(5).plus(id).show();
        assertSimilar("SELECT ? * ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        String sql = param.opposite().where(id.booleanValue()).show();
        assertSimilar("SELECT - ? AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testPlus() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.plus(id).show();
        assertSimilar("SELECT ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.plus(2).plus(id).show();
        assertSimilar("SELECT ? + ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.minus(id).show();
        assertSimilar("SELECT ? - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.minus(1.3).plus(id).show();
        assertSimilar("SELECT ? - ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.div(id).show();
        assertSimilar("SELECT ? / T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.div(4L).plus(id).show();
        assertSimilar("SELECT ? / ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.concat(id).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testString() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.concat(" ").concat(id).show();
        assertSimilar("SELECT ? || ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.union(person.id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testUnionAll() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.unionAll(person.id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testUnionDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.unionDistinct(person.id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testExcept() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.except(person.id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testExceptDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.exceptDistinct(person.id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testExceptAll() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.exceptAll(person.id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testIntersect() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.intersect(person.id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testIntersectAll() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.intersectAll(person.id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testIntersectDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.intersectDistinct(person.id).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testExists() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            person.id.where(param.exists()).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testQueryValue() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            person.id.where(param.queryValue().eq(param)).show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testForUpdate() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.forUpdate().show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testForReadOnly() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.forReadOnly().show();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testList() throws Exception {
        final DataSource dataSource = org.easymock.EasyMock.createMock(DataSource.class);
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
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
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
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


    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "id");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
    }

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

}
