package org.simqle.sql;

import org.simqle.Callback;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class QueryBaseScalarTest extends SqlTestCase {


    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(employee.id.all().booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(employee.id.all().in(manager.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) IN(SELECT ALL T2.id FROM manager AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(employee.id.all().notIn(manager.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) NOT IN(SELECT ALL T2.id FROM manager AS T2)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.name.where(employee.id.all().in(person.id)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) IN(T0.id)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.name.where(employee.id.all().notIn(person.id)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) NOT IN(T0.id)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.name.where(employee.id.all().isNull()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.name.where(employee.id.all().isNotNull()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.name.where(employee.id.all().eq(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) = ?", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.name.where(employee.id.all().ne(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) <> ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.name.where(employee.id.all().lt(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) < ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.name.where(employee.id.all().le(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) <= ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.name.where(employee.id.all().gt(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) > ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.name.where(employee.id.all().ge(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) >= ?", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = employee.id.distinct().opposite().orderBy(person.name).show();
        assertSimilar("SELECT -(SELECT DISTINCT T3.id FROM employee AS T3) AS C1 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testPlus() throws Exception {
        final String sql = employee.id.distinct().plus(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT DISTINCT T3.id FROM employee AS T3) + T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMinus() throws Exception {
        final String sql = employee.id.distinct().minus(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT DISTINCT T3.id FROM employee AS T3) - T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMult() throws Exception {
        final String sql = employee.id.distinct().mult(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT DISTINCT T3.id FROM employee AS T3) * T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testDiv() throws Exception {
        final String sql = employee.id.distinct().div(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT DISTINCT T3.id FROM employee AS T3) / T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }


    public void testPlusNumber() throws Exception {
        final String sql = employee.id.all().plus(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT ALL T3.id FROM employee AS T3) + ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMinusNumber() throws Exception {
        final String sql = employee.id.all().minus(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT ALL T3.id FROM employee AS T3) - ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = employee.id.all().mult(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT ALL T3.id FROM employee AS T3) * ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = employee.id.all().div(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT ALL T3.id FROM employee AS T3) / ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testConcat() throws Exception {
        final String sql = employee.name.all().concat(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT ALL T3.name FROM employee AS T3) || T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = employee.name.all().concat(" test").orderBy(person.name).show();
        assertSimilar("SELECT(SELECT ALL T3.name FROM employee AS T3) || ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testSort() throws Exception {
        final String sql = person.name.orderBy(employee.name.all()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT ALL T1.name FROM employee AS T1)", sql);
    }

    public void testSortAsc() throws Exception {
        final String sql = person.name.orderBy(employee.name.all().asc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT ALL T1.name FROM employee AS T1) ASC", sql);
    }

    public void testSortDesc() throws Exception {
        final String sql = person.name.orderBy(employee.name.all().desc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT ALL T1.name FROM employee AS T1) DESC", sql);
    }

    public void testSortNullsFirst() throws Exception {
        final String sql = person.name.orderBy(employee.name.all().nullsFirst()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT ALL T1.name FROM employee AS T1) NULLS FIRST", sql);
    }

    public void testSortNullsLast() throws Exception {
        final String sql = person.name.orderBy(employee.name.all().nullsLast()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT ALL T1.name FROM employee AS T1) NULLS LAST", sql);
    }


    public void testWhere() throws Exception {
        final String sql = person.id.all().where(person.name.isNotNull()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.id.all().orderBy(person.name).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testAllSelect() throws Exception {
        final String sql = person.id.all().select().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT(SELECT ALL T0.id FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testAllDistinct() throws Exception {
        final String sql = person.id.all().distinct().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT DISTINCT(SELECT ALL T0.id FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testDistinctAll() throws Exception {
        final String sql = person.id.distinct().all().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT ALL(SELECT DISTINCT T0.id FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.all().forUpdate().show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.all().forReadOnly().show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.id.all().union(employee.id.select()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 UNION SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.all().unionAll(employee.id.select()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 UNION ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = person.id.all().unionDistinct(employee.id.select()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExcept() throws Exception {
        final String sql = person.id.all().except(employee.id.select()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 EXCEPT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.all().exceptAll(employee.id.select()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = person.id.all().exceptDistinct(employee.id.select()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = person.id.all().intersect(employee.id.select()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 INTERSECT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.all().intersectAll(employee.id.select()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.all().intersectDistinct(employee.id.select()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExists() throws Exception {
        final String sql = person.id.all().where(employee.name.all().exists()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT ALL T1.name FROM employee AS T1)", sql);

    }


    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.id.all().show();
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

        final List<Long> list = person.id.all().list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.id.all().show();
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

        person.id.all().scroll(datasource, new Callback<Long, SQLException>() {
            int callCount = 0;

            @Override
            public void iterate(final Long aNumber) throws SQLException, BreakException {
                if (callCount++ != 0) {
                    fail("One call expected, actually " + callCount);
                }
                assertEquals(123L, aNumber.longValue());
            }
        });
        verify(datasource, connection,  statement, resultSet);
    }





    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
    }

    private static Person person = new Person();

    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> retired = new LongColumn("retired", this);
    }

    private static Employee employee = new Employee();

    private static class Manager extends Table {
        private Manager() {
            super("manager");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> retired = new LongColumn("retired", this);
    }
    private static Manager manager = new Manager();

    private DynamicParameter<Long> two = new LongParameter(2L);

}
