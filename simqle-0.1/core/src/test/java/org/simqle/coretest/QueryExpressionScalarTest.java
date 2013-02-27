package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.TableOrView;

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


    public void testQueryValueBooleanValue() throws Exception {
        final String sql = person.id.where(employee.id.union(manager.id).queryValue().booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = employee.id.union(manager.id).forUpdate().show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = employee.id.union(manager.id).forReadOnly().show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 FOR READ ONLY", sql);
    }

    public void testUnion() throws Exception {
        final String sql = employee.id.union(manager.id).union(person.id).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = employee.id.union(manager.id).unionAll(person.id).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = employee.id.union(manager.id).unionDistinct(person.id).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExcept() throws Exception {
        final String sql = employee.id.union(manager.id).except(person.id).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = employee.id.union(manager.id).exceptAll(person.id).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = employee.id.union(manager.id).exceptDistinct(person.id).show();
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = employee.id.union(manager.id).intersect(person.id).show();
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = employee.id.union(manager.id).intersectAll(person.id).show();
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = employee.id.union(manager.id).intersectDistinct(person.id).show();
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExists() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.id.eq(person.id)).union(manager.id.where(manager.id.eq(person.id))).exists()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.id = T0.id UNION SELECT T2.id FROM manager AS T2 WHERE T2.id = T0.id)", sql);

    }

    public void testAsInSublist() throws Exception {
        final String sql = person.name.where(person.id.in(employee.id.except(manager.id))).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE T0.id IN(SELECT T1.id FROM employee AS T1 EXCEPT SELECT T2.id FROM manager AS T2)", sql);
    }



    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = employee.id.union(manager.id).show();
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

        final List<Long> list = employee.id.union(manager.id).list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = employee.id.union(manager.id).show();
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

        employee.id.union(manager.id).scroll(datasource, new Callback<Long, SQLException>() {
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

    private static class Manager extends TableOrView {
        private Manager() {
            super("manager");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Manager manager = new Manager();

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
