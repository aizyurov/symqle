package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractQueryPrimary;
import org.simqle.sql.Column;
import org.simqle.sql.DatabaseGate;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.GenericDialect;
import org.simqle.sql.TableOrView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class QueryPrimaryTest extends SqlTestCase {

    public void testShow() throws Exception {
        final String sql = queryPrimary.show();
        final String sql2 = queryPrimary.show(GenericDialect.get());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ?", sql);
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ?", sql2);
    }

    public void testContains() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.contains(1)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testInArgument() throws Exception {
        final String sql = new Employee().name.where(DynamicParameter.create(Mappers.INTEGER, 1).in(queryPrimary)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testExcept() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.except(queryPrimary).contains(1)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? EXCEPT SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.exceptAll(queryPrimary).contains(1)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? EXCEPT ALL SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.exceptDistinct(queryPrimary).contains(1)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? EXCEPT DISTINCT SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.intersect(queryPrimary).contains(1)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? INTERSECT SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.intersectAll(queryPrimary).contains(1)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? INTERSECT ALL SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.intersectDistinct(queryPrimary).contains(1)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? INTERSECT DISTINCT SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testUnion() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.union(queryPrimary).contains(1)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? UNION SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.unionAll(queryPrimary).contains(1)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? UNION ALL SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.unionDistinct(queryPrimary).contains(1)).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? UNION DISTINCT SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testExists() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.exists()).show();
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE EXISTS(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = queryPrimary.forReadOnly().show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? FOR READ ONLY", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = queryPrimary.forUpdate().show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? FOR UPDATE", sql);
    }

    public void testOrderAsc() throws Exception {
        final String sql = queryPrimary.orderAsc().show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? ORDER BY C1", sql);
    }

    public void testOrderDesc() throws Exception {
        final String sql = queryPrimary.orderDesc().show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? ORDER BY C1 DESC", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = queryPrimary.queryValue().orderBy(new Employee().id).show();
        assertSimilar("SELECT(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?) AS C1 FROM employee AS T2 ORDER BY T2.id", sql);
    }

    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DatabaseGate gate, final AbstractQueryPrimary<Integer> queryPrimary) throws SQLException {
                final List<Integer> list = queryPrimary.list(gate);
                assertEquals(Arrays.asList(123), list);
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DatabaseGate gate, final AbstractQueryPrimary<Integer> queryPrimary) throws SQLException {
                queryPrimary.scroll(gate, new Callback<Integer>() {
                    private int callCount = 0;
                    @Override
                    public boolean iterate(final Integer integer) {
                        assertEquals(0, callCount++);
                        assertEquals(Integer.valueOf(123), integer);
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
            final String queryString = queryPrimary.show();
            expect(gate.getDialect()).andReturn(GenericDialect.get());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            statement.setString(1, "A%");
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getInt(matches("[SC][0-9]"))).andReturn(123);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection,  statement, resultSet);

            runQuery(gate, queryPrimary);
            verify(gate, connection,  statement, resultSet);
        }

        protected abstract void runQuery(final DatabaseGate gate, final AbstractQueryPrimary<Integer> queryPrimary) throws SQLException;
    }


    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static final Employee employee = new Employee();
    private static final AbstractQueryPrimary<Integer> queryPrimary = employee.id.count().where(employee.name.like("A%"));


}
