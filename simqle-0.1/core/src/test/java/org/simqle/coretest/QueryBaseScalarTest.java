package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractQueryBaseScalar;
import org.simqle.sql.Column;
import org.simqle.sql.DatabaseGate;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.GenericDialect;
import org.simqle.sql.TableOrView;

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


    public void testShow() throws Exception {
        final String sql = person.id.all().show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0", sql);
        final String sql2 = person.id.all().show(GenericDialect.get());
        assertSimilar(sql, sql2);
    }


    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(employee.id.all().queryValue().booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(employee.id.all().queryValue().in(manager.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) IN(SELECT ALL T2.id FROM manager AS T2)", sql);
    }

    public void testSort() throws Exception {
        final String sql = person.name.orderBy(employee.name.all().queryValue()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT ALL T1.name FROM employee AS T1)", sql);
    }


    public void testWhere() throws Exception {
        final String sql = person.id.all().where(person.name.isNotNull()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.id.all().orderBy(person.name).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 ORDER BY T0.name", sql);
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
        final String sql = person.id.all().union(employee.id).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 UNION SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.all().unionAll(employee.id).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 UNION ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = person.id.all().unionDistinct(employee.id).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExcept() throws Exception {
        final String sql = person.id.all().except(employee.id).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 EXCEPT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.all().exceptAll(employee.id).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = person.id.all().exceptDistinct(employee.id).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = person.id.all().intersect(employee.id).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 INTERSECT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.all().intersectAll(employee.id).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.all().intersectDistinct(employee.id).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExists() throws Exception {
        final String sql = person.id.all().where(employee.name.all().exists()).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT ALL T1.name FROM employee AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = person.id.all().where(employee.name.all().contains("Jim")).show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT ALL T1.name FROM employee AS T1)", sql);

    }

    public void testQueryValue() throws Exception {
        final String sql = person.id.all().queryValue().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT(SELECT ALL T0.id FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }


    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DatabaseGate gate, final AbstractQueryBaseScalar<Long> queryBaseScalar) throws SQLException {
                final List<Long> list = queryBaseScalar.list(gate);
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();

    }


    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DatabaseGate gate, final AbstractQueryBaseScalar<Long> queryBaseScalar) throws SQLException {
                queryBaseScalar.scroll(gate, new Callback<Long>() {
                    int callCount = 0;

                    @Override
                    public boolean iterate(final Long aNumber) {
                        if (callCount++ != 0) {
                            fail("One call expected, actually " + callCount);
                        }
                        assertEquals(123L, aNumber.longValue());
                        return true;
                    }
                });
            }
        }.play();

    }

    private static abstract class Scenario {
        public void play() throws Exception {
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            final AbstractQueryBaseScalar<Long> queryBaseScalar = person.id.all();
            final String queryString = queryBaseScalar.show();
            expect(gate.getDialect()).andReturn(GenericDialect.get());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection, statement, resultSet);

            runQuery(gate, queryBaseScalar);
            verify(gate, connection, statement, resultSet);
        }

        protected abstract void runQuery(final DatabaseGate gate, final AbstractQueryBaseScalar<Long> queryBaseScalar) throws SQLException;
    }





    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Employee employee = new Employee();

    private static class Manager extends TableOrView {
        private Manager() {
            super("manager");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }
    private static Manager manager = new Manager();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
