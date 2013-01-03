package org.simqle.sql;

import org.simqle.Callback;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.easymock.EasyMock.*;
import static org.easymock.EasyMock.expect;

/**
 * @author lvovich
 */
public class ValueExpressionPrimaryTest extends SqlTestCase {

    // show is not applicable to subquery
    public void testShow() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testAll() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().all().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testDistinct() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().distinct().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testWhere() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().eq(employee.id).asValue().show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) = T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().ne(employee.id).asValue().show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <> T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().gt(employee.id).asValue().show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) > T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().ge(employee.id).asValue().show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) >= T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().lt(employee.id).asValue().show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) < T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().le(employee.id).asValue().show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <= T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().pair(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0, T1.id AS C1 FROM employee AS T1", sql);
    }
    public void testPlus() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().plus(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testMinus() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().minus(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) - T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testMult() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().mult(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) * T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testDiv() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().div(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) / T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testPlusNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().plus(2).plus(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) + ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testMinusNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().minus(2).plus(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) - ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().mult(2).plus(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) * ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().div(2).plus(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) / ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testConcat() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().concat(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) || T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().concat(":").concat(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) || ? || T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testForReadOnly() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().forReadOnly().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testForUpdate() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().forUpdate().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testOrderBy() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().orderBy(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0 FROM employee AS T1 ORDER BY T1.id", sql);
    }

    public void testAsSortSpecification() throws Exception {
        final String sql = employee.id.orderBy(person.id.where(person.name.eq(employee.name)).queryValue()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)", sql);
    }

    public void testAsc() throws Exception {
        final String sql = employee.id.orderBy(person.id.where(person.name.eq(employee.name)).queryValue().asc()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) ASC", sql);
    }

    public void testDesc() throws Exception {
        final String sql = employee.id.orderBy(person.id.where(person.name.eq(employee.name)).queryValue().desc()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) DESC", sql);
    }

    public void testNullsFirst() throws Exception {
        final String sql = employee.id.orderBy(person.id.where(person.name.eq(employee.name)).queryValue().nullsFirst()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) NULLS FIRST", sql);
    }

    public void testNullsLast() throws Exception {
        final String sql = employee.id.orderBy(person.id.where(person.name.eq(employee.name)).queryValue().nullsLast()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) NULLS LAST", sql);
    }

    public void testUnion() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().union(employee.id).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testUnionAll() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().unionAll(employee.id).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testUnionDistinct() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().unionDistinct(employee.id).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }


    public void testExcept() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().except(employee.id).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testExceptAll() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().exceptAll(employee.id).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testExceptDistinct() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().exceptDistinct(employee.id).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testIntersect() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().intersect(employee.id).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testIntersectAll() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().intersectAll(employee.id).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testIntersectDistinct() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().intersectDistinct(employee.id).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testExists() throws Exception {
        try {
            final String sql = employee.id.where(person.id.where(person.name.eq(employee.name)).queryValue().exists()).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testAsInArgument() throws Exception {
        try {
            final String sql = employee.id.where(employee.id.in(person.id.where(person.name.eq(employee.name)).queryValue())).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testAsInListArguments() throws Exception {
        final String sql = employee.id.where(
                employee.id.in(
                        person.id.where(person.name.eq(employee.name)).queryValue(),
                        person.id.where(person.name.eq(employee.name)).queryValue().opposite())).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 WHERE T1.id IN((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name), -(SELECT T2.id FROM person AS T2 WHERE T2.name = T1.name))", sql);
    }

    public void testQueryValue() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().queryValue().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        replay(datasource);
        try {
            final List<Long> list = person.id.where(person.name.eq(employee.name)).queryValue().list(datasource);
            fail ("IllegalStateException expected but produced: "+list);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testQuery() throws Exception {
        final AbstractQuerySpecification<Long> querySpecification = person.id.where(person.name.eq(employee.name)).queryValue().where(employee.name.isNotNull());
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = querySpecification.show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);
        final List<Long> list = querySpecification.list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
    }

    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        replay(datasource);
        try {
            person.id.where(person.name.eq(employee.name)).queryValue().scroll(datasource, new Callback<Long, SQLException>() {
                @Override
                public void iterate(final Long aLong) throws SQLException, BreakException {
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
    }

    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
    }

    private static Person person = new Person();
    private static Employee employee = new Employee();

}
