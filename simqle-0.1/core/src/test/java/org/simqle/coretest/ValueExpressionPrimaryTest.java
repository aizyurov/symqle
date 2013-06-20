package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractQuerySpecification;
import org.simqle.sql.AbstractValueExpressionPrimary;
import org.simqle.sql.Column;
import org.simqle.sql.DatabaseGate;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.GenericDialect;
import org.simqle.sql.TableOrView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.easymock.EasyMock.*;

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
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().show(GenericDialect.get());
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testMap() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().map(Mappers.STRING).show();
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

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().eq(1L).asValue().orderBy(employee.name).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) = ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().ne(1L).asValue().orderBy(employee.name).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <> ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().gt(1L).asValue().orderBy(employee.name).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) > ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().ge(1L).asValue().orderBy(employee.name).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) >= ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().lt(1L).asValue().orderBy(employee.name).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) < ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().le(1L).asValue().orderBy(employee.name).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <= ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().pair(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0, T1.id AS C1 FROM employee AS T1", sql);
    }
    public void testAdd() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().add(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testSub() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().sub(employee.id).show();
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

    public void testAddNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().add(2).add(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) + ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testSubNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().sub(2).add(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) - ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().mult(2).add(employee.id).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) * ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().div(2).add(employee.id).show();
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
            final AbstractValueExpressionPrimary<Long> vep = person.id.where(person.name.eq(employee.name)).queryValue();
            final String sql = employee.id.where(employee.id.in(vep)).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testContains() throws Exception {
        try {
            final AbstractValueExpressionPrimary<Long> vep = person.id.where(person.name.eq(employee.name)).queryValue();
            final String sql = employee.id.where(vep.contains(1L)).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testQueryValue() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().queryValue().where(employee.id.eq(1L)).show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(employee.name.where(employee.id.eq(person.id)).queryValue()).show();
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.name).orElse(employee.name.where(employee.id.eq(person.id)).queryValue()).show();
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name ELSE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) END AS C0 FROM person AS T0", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().like(DynamicParameter.create(Mappers.STRING, "true"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().notLike(DynamicParameter.create(Mappers.STRING, "true"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().like("true")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().notLike("true")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) NOT LIKE ?", sql);
    }


    public void testList() throws Exception {
        final DatabaseGate gate = createMock(DatabaseGate.class);
        expect(gate.getDialect()).andReturn(GenericDialect.get());
        replay(gate);
        try {
            final List<Long> list = person.id.where(person.name.eq(employee.name)).queryValue().list(gate);
            fail ("IllegalStateException expected but produced: "+list);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
        verify(gate);

    }

    public void testCount() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().count().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT COUNT((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().countDistinct().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT COUNT(DISTINCT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().avg().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT AVG((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().sum().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT SUM((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().min().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT MIN((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().max().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT MAX((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }



    public void testQuery() throws Exception {
        final AbstractQuerySpecification<Long> querySpecification = person.id.where(person.name.eq(employee.name)).queryValue().where(employee.name.isNotNull());
        final DatabaseGate datasource = createMock(DatabaseGate.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = querySpecification.show();
        expect(datasource.getDialect()).andReturn(GenericDialect.get());
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
        final DatabaseGate datasource = createMock(DatabaseGate.class);
        expect(datasource.getDialect()).andReturn(GenericDialect.get());
        replay(datasource);
        try {
            person.id.where(person.name.eq(employee.name)).queryValue().scroll(datasource, new Callback<Long>() {
                @Override
                public boolean iterate(final Long aLong) {
                    fail("must not get here");
                    return true;
                }
            });
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
        verify(datasource);

    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();
    private static Employee employee = new Employee();

}
