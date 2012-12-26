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
public class QuerySpecificationTest extends SqlTestCase {


    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.isNotNull()).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL)", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.isNotNull()).in(manager.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) IN(SELECT ALL T2.id FROM manager AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.isNotNull()).notIn(manager.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) NOT IN(SELECT ALL T2.id FROM manager AS T2)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.name.isNotNull()).in(person.id)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) IN(T0.id)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.name.isNotNull()).notIn(person.id)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) NOT IN(T0.id)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.name.isNotNull()).isNull()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.name.isNotNull()).isNotNull()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.name.isNotNull()).eq(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) = ?", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.name.isNotNull()).ne(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) <> ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.name.isNotNull()).lt(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) < ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.name.isNotNull()).le(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) <= ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.name.isNotNull()).gt(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) > ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.name.isNotNull()).ge(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 WHERE T1.name IS NOT NULL) >= ?", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = employee.id.where(employee.name.isNotNull()).opposite().select().orderBy(person.name).show();
        assertSimilar("SELECT -(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) AS C1 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testPlus() throws Exception {
        final String sql = employee.id.where(employee.name.isNotNull()).plus(person.id).select().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) + T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMinus() throws Exception {
        final String sql = employee.id.where(employee.name.isNotNull()).minus(person.id).select().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) - T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMult() throws Exception {
        final String sql = employee.id.where(employee.name.isNotNull()).mult(person.id).select().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) * T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testDiv() throws Exception {
        final String sql = employee.id.where(employee.name.isNotNull()).div(person.id).select().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) / T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }


    public void testPlusNumber() throws Exception {
        final String sql = employee.id.where(employee.name.isNotNull()).plus(2).select().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) + ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMinusNumber() throws Exception {
        final String sql = employee.id.where(employee.name.isNotNull()).minus(2).select().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) - ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = employee.id.where(employee.name.isNotNull()).mult(2).select().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) * ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = employee.id.where(employee.name.isNotNull()).div(2).select().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) / ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testConcat() throws Exception {
        final String sql = employee.name.where(employee.name.isNotNull()).concat(person.id).select().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.name FROM employee AS T3 WHERE T3.name IS NOT NULL) || T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = employee.name.where(employee.name.isNotNull()).concat(" test").select().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.name FROM employee AS T3 WHERE T3.name IS NOT NULL) || ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testSort() throws Exception {
        final String sql = person.name.select().orderBy(employee.name.where(employee.name.isNotNull())).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1 WHERE T1.name IS NOT NULL)", sql);
    }

    public void testSortAsc() throws Exception {
        final String sql = person.name.select().orderBy(employee.name.where(employee.name.isNotNull()).asc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1 WHERE T1.name IS NOT NULL) ASC", sql);
    }

    public void testSortDesc() throws Exception {
        final String sql = person.name.select().orderBy(employee.name.where(employee.name.isNotNull()).desc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1 WHERE T1.name IS NOT NULL) DESC", sql);
    }

    public void testSortNullsFirst() throws Exception {
        final String sql = person.name.select().orderBy(employee.name.where(employee.name.isNotNull()).nullsFirst()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1 WHERE T1.name IS NOT NULL) NULLS FIRST", sql);
    }

    public void testSortNullsLast() throws Exception {
        final String sql = person.name.select().orderBy(employee.name.where(employee.name.isNotNull()).nullsLast()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1 WHERE T1.name IS NOT NULL) NULLS LAST", sql);
    }


    public void testWhereWhere() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).where(employee.name.isNull()).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name IS NOT NULL) AS C0 FROM employee AS T1 WHERE T1.name IS NULL", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).orderBy(person.name).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL ORDER BY T0.name", sql);
    }

    public void testWhereSelect() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).select().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name IS NOT NULL) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testWhereDistinct() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).distinct().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT DISTINCT(SELECT T0.id FROM person AS T0 WHERE T0.name IS NOT NULL) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testWhereAll() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).all().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT ALL(SELECT T0.id FROM person AS T0 WHERE T0.name IS NOT NULL) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).forUpdate().show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).forReadOnly().show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL FOR READ ONLY", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).union(employee.id.select()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL UNION SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).unionAll(employee.id.select()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL UNION ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).unionDistinct(employee.id.select()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL UNION DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExcept() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).except(employee.id.select()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL EXCEPT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).exceptAll(employee.id.select()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL EXCEPT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).exceptDistinct(employee.id.select()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL EXCEPT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).intersect(employee.id.select()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL INTERSECT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).intersectAll(employee.id.select()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL INTERSECT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).intersectDistinct(employee.id.select()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL INTERSECT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.id.where(person.name.eq(employee.name)).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id FROM person AS T1 WHERE T1.name = T0.name)", sql);

    }


    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.id.where(person.name.isNull()).show();
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

        final List<Long> list = person.id.where(person.name.isNull()).list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.id.where(person.name.isNull()).show();
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

        person.id.where(person.name.isNull()).scroll(datasource, new Callback<Long, SQLException>() {
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
