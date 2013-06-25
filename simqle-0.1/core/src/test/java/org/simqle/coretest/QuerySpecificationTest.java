package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractQuerySpecification;
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
public class QuerySpecificationTest extends SqlTestCase {


    public void testShow() throws Exception {
        final AbstractQuerySpecification<Long> querySpecification = person.id.where(person.name.isNotNull());
        final String sql = querySpecification.show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
        assertSimilar(sql, querySpecification.show(GenericDialect.get()));
    }

    public void testQueryValue() throws Exception {
        final String sql = employee.id.where(employee.name.isNotNull()).queryValue().orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) AS C1 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testOrderAsc() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).orderAsc().show();
        assertSimilar("SELECT T0.id AS S0 FROM person AS T0 WHERE T0.name IS NOT NULL ORDER BY S0", sql);
    }

    public void testOrderDesc() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).orderDesc().show();
        assertSimilar("SELECT T0.id AS S0 FROM person AS T0 WHERE T0.name IS NOT NULL ORDER BY S0 DESC", sql);
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
        final String sql = person.id.where(person.name.isNotNull()).union(employee.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL UNION SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).unionAll(employee.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL UNION ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).unionDistinct(employee.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL UNION DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExcept() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).except(employee.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL EXCEPT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).exceptAll(employee.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL EXCEPT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).exceptDistinct(employee.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL EXCEPT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).intersect(employee.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL INTERSECT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).intersectAll(employee.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL INTERSECT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).intersectDistinct(employee.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL INTERSECT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.id.where(person.name.eq(employee.name)).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id FROM person AS T1 WHERE T1.name = T0.name)", sql);

    }

    public void testContains() throws Exception {
        final String sql = employee.id.where(person.id.where(person.name.eq(employee.name)).contains(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT T1.id FROM person AS T1 WHERE T1.name = T0.name)", sql);
    }


    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DatabaseGate gate, final AbstractQuerySpecification<Long> querySpecification) throws SQLException {
                final List<Long> list = querySpecification.list(gate);
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();

    }


    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DatabaseGate gate, final AbstractQuerySpecification<Long> querySpecification) throws SQLException {
                querySpecification.scroll(gate, new Callback<Long>() {
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
            final AbstractQuerySpecification<Long> querySpecification = person.id.where(person.name.isNull());
            final String queryString = querySpecification.show();
            expect(gate.getDialect()).andReturn(GenericDialect.get());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getLong(matches("[SC][0-9]"))).andReturn(123L);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection,  statement, resultSet);

            runQuery(gate, querySpecification);
            verify(gate, connection,  statement, resultSet);
        }

        protected abstract void runQuery(final DatabaseGate gate, final AbstractQuerySpecification<Long> querySpecification) throws SQLException;
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

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
