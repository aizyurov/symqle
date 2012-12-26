package org.simqle.sql;

import org.simqle.Callback;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class QueryExpressionScalarTest extends SqlTestCase {


    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(employee.id.select().union(manager.id.select()).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2)", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(employee.id.select().union(manager.id.select()).in(person2.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) IN(SELECT ALL T3.id FROM person AS T3)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(employee.id.select().union(manager.id.select()).notIn(person2.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) NOT IN(SELECT ALL T3.id FROM person AS T3)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.name.where(employee.id.select().union(manager.id.select()).in(person.id)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) IN(T0.id)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.name.where(employee.id.select().union(manager.id.select()).notIn(person.id)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) NOT IN(T0.id)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.name.where(employee.id.select().union(manager.id.select()).isNull()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.name.where(employee.id.select().union(manager.id.select()).isNotNull()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.name.where(employee.id.select().union(manager.id.select()).eq(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) = ?", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.name.where(employee.id.select().union(manager.id.select()).ne(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) <> ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.name.where(employee.id.select().union(manager.id.select()).lt(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) < ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.name.where(employee.id.select().union(manager.id.select()).le(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) <= ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.name.where(employee.id.select().union(manager.id.select()).gt(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) > ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.name.where(employee.id.select().union(manager.id.select()).ge(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) >= ?", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).opposite().where(person.name.isNull()).show();
        assertSimilar("SELECT -(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) AS C1 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testPlus() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).plus(person.id).where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) + T3.id AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testMinus() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).minus(person.id).where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) - T3.id AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testMult() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).mult(person.id).where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) * T3.id AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testDiv() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).div(person.id).where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) / T3.id AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }


    public void testPlusNumber() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).plus(2).where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) + ? AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testMinusNumber() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).minus(2).where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) - ? AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).mult(2).where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) * ? AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).div(2).where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) / ? AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testConcat() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).concat(person.id).where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) || T3.id AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).concat(" test").where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) || ? AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testWhere() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).where(person.name.isNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) AS C0 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testIntersectSelect() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).select().where(person.name.isNotNull()).show();
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testDistinct() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).distinct().where(person.name.isNotNull()).show();
        assertSimilar("SELECT DISTINCT(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testWhereAll() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).all().where(person.name.isNotNull()).show();
        assertSimilar("SELECT ALL(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2) AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).forUpdate().show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).forReadOnly().show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 FOR READ ONLY", sql);
    }

    public void testUnion() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).union(person.id.select()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).unionAll(person.id.select()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).unionDistinct(person.id.select()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExcept() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).except(person.id.select()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).exceptAll(person.id.select()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).exceptDistinct(person.id.select()).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).intersect(person.id.select()).show();
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).intersectAll(person.id.select()).show();
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = employee.id.select().union(manager.id.select()).intersectDistinct(person.id.select()).show();
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExists() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.id.eq(person.id)).union(manager.id.where(manager.id.eq(person.id))).exists()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.id = T0.id UNION SELECT T2.id FROM manager AS T2 WHERE T2.id = T0.id)", sql);

    }


    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = employee.id.select().union(manager.id.select()).show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong("S0")).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        final List<Long> list = employee.id.select().union(manager.id.select()).list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = employee.id.select().union(manager.id.select()).show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong("S0")).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        employee.id.select().union(manager.id.select()).scroll(datasource, new Callback<Long, SQLException>() {
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

    private static Person person2 = new Person();

    private DynamicParameter<Long> two = new LongParameter(2L);

}
