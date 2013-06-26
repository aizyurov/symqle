package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractAggregateFunction;
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
public class AggregatesTest extends SqlTestCase  {

    public void testShow() throws Exception {
        final String show = person.id.count().show();
        final String show2 = person.id.count().show(GenericDialect.get());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1", show);
        assertSimilar(show, show2);
    }

    public void testUnion() throws Exception {
        final String show = person.id.count().union(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 UNION SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testUnionAll() throws Exception {
        final String show = person.id.count().unionAll(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 UNION ALL SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }
    public void testUnionDistinct() throws Exception {
        final String show = person.id.count().unionDistinct(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 UNION DISTINCT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testExcept() throws Exception {
        final String show = person.id.count().except(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 EXCEPT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testExceptAll() throws Exception {
        final String show = person.id.count().exceptAll(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 EXCEPT ALL SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testExceptDistinct() throws Exception {
        final String show = person.id.count().exceptDistinct(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 EXCEPT DISTINCT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testIntersect() throws Exception {
        final String show = person.id.count().intersect(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 INTERSECT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testIntersectAll() throws Exception {
        final String show = person.id.count().intersectAll(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 INTERSECT ALL SELECT COUNT(T2.parent_id) AS C2 FROM person AS T2", show);
    }

    public void testIntersectDistinct() throws Exception {
        final AbstractAggregateFunction<Integer> count = person.id.count();
        final String show = count.intersectDistinct(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 INTERSECT DISTINCT SELECT COUNT(T2.parent_id) AS C2 FROM person AS T2", show);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.count().forUpdate().show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.count().forReadOnly().show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 FOR READ ONLY", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.count().where(person.name.isNull()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.name IS NULL", sql);
    }

    public void testOrderAsc() throws Exception {
        final String sql = person.id.count().orderAsc().show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 ORDER BY C1 ASC", sql);
    }

    public void testOrderDesc() throws Exception {
        final String sql = person.id.count().orderDesc().show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 ORDER BY C1 DESC", sql);
    }

    public void testExists() throws Exception {
        // rather meaningless
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(child.age.sum().exists()).show();
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE EXISTS(SELECT SUM(T2.age) FROM person AS T2)", sql);
    }

    public void testContains() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(child.age.sum().contains(1)).show();
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE ? IN(SELECT SUM(T2.age) FROM person AS T2)", sql);
    }

    public void testIn() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(DynamicParameter.create(Mappers.INTEGER, 1).in(child.id.count())).show();
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE ? IN(SELECT COUNT(T2.id) FROM person AS T2)", sql);
    }

    public void testQueryValue() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.pair(child.id.count().queryValue()).show();
        assertSimilar("SELECT T1.name AS C1,(SELECT COUNT(T2.id) FROM person AS T2) AS C2 FROM person AS T1", sql);
    }

    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final AbstractAggregateFunction<Integer> count, final DatabaseGate gate) throws SQLException {
                final List<Integer> list = count.list(gate);
                assertEquals(1, list.size());
                assertEquals(123, list.get(0).intValue());
            }
        }.play();

    }

    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final AbstractAggregateFunction<Integer> count, final DatabaseGate gate) throws SQLException {
                count.scroll(gate, new Callback<Integer>() {
                    private int callCount = 0;

                    @Override
                    public boolean iterate(final Integer integer) {
                        if (callCount > 0) {
                            fail("Only one call expected");
                        }
                        assertEquals(integer.intValue(), 123);
                        return true;
                    }
                });
            }
        }.play();

    }


    private static abstract class Scenario {
        public void play() throws Exception {
            final AbstractAggregateFunction<Integer> count = person.id.count();
            final String queryString = count.show();
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            expect(gate.getDialect()).andReturn(GenericDialect.get());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getInt(matches("[SC][0-9]"))).andReturn(123);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection,  statement, resultSet);

            runQuery(count, gate);

            verify(gate, connection, statement, resultSet);
        }

        protected abstract void runQuery(final AbstractAggregateFunction<Integer> count, final DatabaseGate gate) throws SQLException;
    }


    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
    }

    private static Person person = new Person();

}
