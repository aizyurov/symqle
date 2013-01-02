package org.simqle.sql;

import org.simqle.Callback;
import org.simqle.Mappers;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;

/**
 * @author lvovich
 */
public class ExistsPredicateTest extends SqlTestCase {


    public void testSelect() throws Exception {
        try {
            final String sql = person.id.exists().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testAll() throws Exception {
        try {
            final String sql = person.id.exists().all().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testDistinct() throws Exception {
        try {
            final String sql = person.id.exists().distinct().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testPair() throws Exception {
        final String sql = person.id.exists().pair(employee.name).show();
        assertSimilar("SELECT EXISTS(SELECT T0.id FROM person AS T0) AS C0, T1.name AS C1 FROM employee AS T1", sql);
    }

    public void testPredicate() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)", sql);
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().and(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) AND T0.id IS NOT NULL", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name))", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().or(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) OR T0.id IS NOT NULL", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().in(employee2.id.isNotNull().where(employee2.name.eq(person.name)))).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) IN(SELECT T2.id IS NOT NULL FROM employee AS T2 WHERE T2.name = T0.name)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().in(person.name.isNotNull(), person.name.isNull())).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) IN(T0.name IS NOT NULL, T0.name IS NULL)", sql);
    }

    public void testAsInValue() throws Exception {
        try {
            final String sql = person.id.where(employee.id.isNotNull().in(employee.id.where(employee.name.eq(person.name)).exists())).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().notIn(employee2.id.isNotNull().where(employee2.name.eq(person.name)))).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) NOT IN(SELECT T2.id IS NOT NULL FROM employee AS T2 WHERE T2.name = T0.name)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().notIn(person.name.isNotNull(), person.name.isNull())).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) NOT IN(T0.name IS NOT NULL, T0.name IS NULL)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) IS NOT NULL", sql);
    }

    public void testWhere() throws Exception {
        final String sql = employee.id.where(employee.name.eq(person.name)).exists().where(person.name.isNotNull()).show();
        assertSimilar("SELECT EXISTS(SELECT T0.id FROM employee AS T0 WHERE T0.name = T1.name) AS C0 FROM person AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().eq(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) =(T0.married)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().ne(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) <>(T0.married)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().gt(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) >(T0.married)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().ge(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) >=(T0.married)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().lt(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) <(T0.married)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().le(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) <=(T0.married)", sql);
    }

    public void testPlus() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().plus(two).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE((EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) + ?)", sql);
    }

    public void testPlusNumber() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().plus(2).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE((EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) + ?)", sql);
    }


    public void testMinus() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().minus(two).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE((EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) - ?)", sql);
    }

    public void testMinusNumber() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().minus(2).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE((EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) - ?)", sql);
    }


    public void testMult() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().mult(two).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE((EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) * ?)", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().mult(2).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE((EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) * ?)", sql);
    }

    public void testDiv() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().div(two).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE((EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) / ?)", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().div(2).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE((EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) / ?)", sql);
    }

    public void testConcat() throws Exception {
        final String sql = employee.id.where(employee.name.eq(person.name)).exists().concat(person.name).show();
        assertSimilar("SELECT(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) || T0.name AS C0 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = employee.id.where(employee.name.eq(person.name)).exists().opposite().orderBy(person.name).show();
        assertSimilar("SELECT -(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) AS C0 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = employee.id.where(employee.name.eq(person.name)).exists().concat(" exists").where(person.name.isNotNull()).show();
        assertSimilar("SELECT(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) || ? AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testSort() throws Exception {
        final String sql = person.id.orderBy(employee.id.where(employee.name.eq(person.name)).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)", sql);
    }

    public void testSortAsc() throws Exception {
        final String sql = person.id.orderBy(employee.id.where(employee.name.eq(person.name)).exists().asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) ASC", sql);
    }

    public void testSortDesc() throws Exception {
        final String sql = person.id.orderBy(employee.id.where(employee.name.eq(person.name)).exists().desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) DESC", sql);
    }

    public void testNullsFirst() throws Exception {
        final String sql = person.id.orderBy(employee.id.where(employee.name.eq(person.name)).exists().nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) NULLS FIRST", sql);
    }

    public void testNullsLast() throws Exception {
        final String sql = person.id.orderBy(employee.id.where(employee.name.eq(person.name)).exists().nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) NULLS LAST", sql);
    }

    public void testUnion() throws Exception {
            try {
                final String sql = person.id.exists().union(employee.retired.booleanValue()).show();
                fail ("IllegalStateException expected but produced: "+sql);
            } catch (IllegalStateException e) {
                assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
            }
    }

    public void testUnionAll() throws Exception {
            try {
                final String sql = person.id.exists().unionAll(employee.retired.booleanValue()).show();
                fail ("IllegalStateException expected but produced: "+sql);
            } catch (IllegalStateException e) {
                assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
            }
    }

    public void testUnionDistinct() throws Exception {
            try {
                final String sql = person.id.exists().unionDistinct(employee.retired.booleanValue()).show();
                fail ("IllegalStateException expected but produced: "+sql);
            } catch (IllegalStateException e) {
                assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
            }
    }

    public void testExcept() throws Exception {
            try {
                final String sql = person.id.exists().except(employee.retired.booleanValue()).show();
                fail ("IllegalStateException expected but produced: "+sql);
            } catch (IllegalStateException e) {
                assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
            }
    }

    public void testExceptAll() throws Exception {
            try {
                final String sql = person.id.exists().exceptAll(employee.retired.booleanValue()).show();
                fail ("IllegalStateException expected but produced: "+sql);
            } catch (IllegalStateException e) {
                assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
            }
    }

    public void testExceptDistinct() throws Exception {
            try {
                final String sql = person.id.exists().exceptDistinct(employee.retired.booleanValue()).show();
                fail ("IllegalStateException expected but produced: "+sql);
            } catch (IllegalStateException e) {
                assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
            }
    }

    public void testIntersect() throws Exception {
            try {
                final String sql = person.id.exists().intersect(employee.retired.booleanValue()).show();
                fail ("IllegalStateException expected but produced: "+sql);
            } catch (IllegalStateException e) {
                assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
            }
    }

    public void testIntersectAll() throws Exception {
            try {
                final String sql = person.id.exists().intersectAll(employee.retired.booleanValue()).show();
                fail ("IllegalStateException expected but produced: "+sql);
            } catch (IllegalStateException e) {
                assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
            }
    }

    public void testIntersectDistinct() throws Exception {
            try {
                final String sql = person.id.exists().intersectDistinct(employee.retired.booleanValue()).show();
                fail ("IllegalStateException expected but produced: "+sql);
            } catch (IllegalStateException e) {
                assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
            }
    }

    public void testExists() throws Exception {
        try {
            final String sql = employee.id.where(employee.name.eq(person.name)).exists().exists().orderBy(person.name).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }

    }

    public void testForUpdate() throws Exception {
        try {
            final String sql = person.id.exists().forUpdate().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testForReadOnly() throws Exception {
        try {
            final String sql = person.id.exists().forReadOnly().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testQueryValue() throws Exception {
        try {
            final String sql = person.id.exists().queryValue().where(employee.retired.booleanValue()).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        replay(datasource);
        try {
            final List<Boolean> list = person.id.exists().list(datasource);
            fail ("IllegalStateException expected but produced: "+ list);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        replay(datasource);
        try {
            person.id.exists().scroll(datasource, new Callback<Boolean, SQLException>() {
                @Override
                public void iterate(final Boolean aBoolean) throws SQLException, BreakException {
                    fail("must not get here");
                }
            });
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }
    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> married = new LongColumn("married", this);
    }

    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> retired = new LongColumn("retired", this);
    }

    private static class Manager extends Table {
        private Manager() {
            super("manager");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> retired = new LongColumn("retired", this);
    }

    private static Person person = new Person();
    private static Employee employee = new Employee();
    private static Employee employee2 = new Employee();
    private static Manager manager = new Manager();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);




}
