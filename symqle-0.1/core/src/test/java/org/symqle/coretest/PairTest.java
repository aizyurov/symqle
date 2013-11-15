package org.symqle.coretest;

import org.symqle.common.MalformedStatementException;
import org.symqle.common.Mappers;
import org.symqle.common.Pair;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractQueryBase;
import org.symqle.sql.AbstractQueryExpression;
import org.symqle.sql.AbstractSelectList;
import org.symqle.sql.Column;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import static org.easymock.EasyMock.*;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 01.01.2013
 * Time: 13:30:17
 * To change this template use File | Settings | File Templates.
 */
public class PairTest extends SqlTestCase {

    public void testShow() throws Exception {
        final String sql = person.id.pair(person.name).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
        final String sql2 = person.id.pair(person.name).show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testPairChain() throws Exception {
        final String sql = person.id.pair(person.name).pair(person.age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1, T0.age AS C2 FROM person AS T0", sql);

    }

    public void testPairArgument() throws Exception {
        final String sql = person.id.pair(person.name.pair(person.age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1, T0.age AS C2 FROM person AS T0", sql);
    }

    public void testAll() throws Exception {
        final String sql = person.id.pair(person.name).selectAll().show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
        final String sql2 = person.id.pair(person.name).selectAll().show(new GenericDialect());
        assertSimilar(sql, sql2);

    }

    public void testDistinct() throws Exception {
        final String sql = person.id.pair(person.name).distinct().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final AbstractQueryExpression<Pair<Long, String>> queryExpression = person.id.pair(person.name).where(person.name.isNotNull());
        final String sql = queryExpression.show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
        final String sql2 = queryExpression.show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.id.pair(person.name).orderBy(person.name).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.pair(person.name).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.pair(person.name).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testDistinctWhere() throws Exception {
        final String sql = person.id.pair(person.name).distinct().where(person.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testDistinctOrderBy() throws Exception {
        final String sql = person.id.pair(person.name).distinct().orderBy(person.name).show(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testDistinctForUpdate() throws Exception {
        final String sql = person.id.pair(person.name).distinct().forUpdate().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testDistinctForReadOnly() throws Exception {
        final String sql = person.id.pair(person.name).distinct().forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testWhereOrderBy() throws Exception {
        final String sql = person.id.pair(person.name).where(person.name.isNotNull()).orderBy(person.name).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL ORDER BY T0.name", sql);
    }

    public void testWhereForUpdate() throws Exception {
        final String sql = person.id.pair(person.name).where(person.name.isNotNull()).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL FOR UPDATE", sql);
    }

    public void testWhereForReadOnly() throws Exception {
        final String sql = person.id.pair(person.name).where(person.name.isNotNull()).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL FOR READ ONLY", sql);
    }

    public void testOrderByForUpdate() throws Exception {
        final String sql = person.id.pair(person.name).orderBy(person.name).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 ORDER BY T0.name FOR UPDATE", sql);
    }

    public void testOrderByForReadOnly() throws Exception {
        final String sql = person.id.pair(person.name).orderBy(person.name).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 ORDER BY T0.name FOR READ ONLY", sql);
    }

    public void testImplicitCrossJoin() throws Exception {
        final Person person1 = new Person();
        final Person person2 = new Person();
        final String badSql;
        try {
            badSql = person1.name.pair(person2.age).where(person1.id.eq(person2.id)).show(new GenericDialect());
            fail("expected IllegalStateException but was " + badSql);
        } catch (MalformedStatementException e) {
            // fine
        }
        final String goodSql = person1.name.pair(person2.age).where(person1.id.eq(person2.id)).show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
        assertSimilar("SELECT T1.name AS C1, T2.age AS C2 FROM person AS T1, person AS T2 WHERE T1.id = T2.id", goodSql);
    }




//    public void testList() throws Exception {
//        new PairScenario() {
//            @Override
//            protected void runQuery(final AbstractSelectList<Pair<Long, String>> selectList, final DatabaseGate gate) throws SQLException {
//                final List<Pair<Long, String>> list = selectList.list(gate);
//                assertEquals(1, list.size());
//                assertEquals(Pair.make(123L, "John"), list.get(0));
//            }
//        }.play();
//
//    }
//
//
//    public void testScroll() throws Exception {
//        new PairScenario() {
//            @Override
//            protected void runQuery(final AbstractSelectList<Pair<Long, String>> selectList, final DatabaseGate gate) throws SQLException {
//                selectList.scroll(gate, new Callback<Pair<Long, String>>() {
//                    int callCount = 0;
//
//                    @Override
//                    public boolean iterate(final Pair<Long, String> pair) {
//                        if (callCount++ != 0) {
//                            fail("One call expected, actually " + callCount);
//                        }
//                        assertEquals(Pair.make(123L, "John"), pair);
//                        return true;
//                    }
//                });
//            }
//        }.play();
//
//    }

    private static abstract class PairScenario {
        public void play() throws Exception {
            final AbstractSelectList<Pair<Long,String>> selectList = person.id.pair(person.name);
            final String queryString = selectList.show(new GenericDialect());
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(new GenericDialect());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.getString(matches("C[0-9]"))).andReturn("John");
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection,  statement, resultSet);

            runQuery(selectList, gate);
            verify(gate, connection,  statement, resultSet);
        }

        protected abstract void runQuery(final AbstractSelectList<Pair<Long, String>> selectList, final DatabaseGate gate) throws SQLException;
    }

//    public void testDistinctList() throws Exception {
//        new DistinctScenario() {
//            @Override
//            protected void runQuery(final AbstractQueryBase<Pair<Long, String>> queryBase, final DatabaseGate gate) throws SQLException {
//                final List<Pair<Long, String>> list = queryBase.list(gate);
//                assertEquals(1, list.size());
//                assertEquals(Pair.make(123L, "John"), list.get(0));
//            }
//        }.play();
//
//    }
//
//    public void testDistinctScroll() throws Exception {
//
//        new DistinctScenario() {
//            @Override
//            protected void runQuery(final AbstractQueryBase<Pair<Long, String>> queryBase, final DatabaseGate gate) throws SQLException {
//                queryBase.scroll(gate, new Callback<Pair<Long, String>>() {
//                    int callCount = 0;
//
//                    @Override
//                    public boolean iterate(final Pair<Long, String> pair) {
//                        if (callCount++ != 0) {
//                            fail("One call expected, actually " + callCount);
//                        }
//                        assertEquals(Pair.make(123L, "John"), pair);
//                        return true;
//                    }
//                });
//            }
//        }.play();
//
//    }

    private static abstract class DistinctScenario {
        public void play() throws Exception {
            final AbstractQueryBase<Pair<Long, String>> queryBase = person.id.pair(person.name).distinct();
            final String queryString = queryBase.show(new GenericDialect());
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(new GenericDialect());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.getString(matches("C[0-9]"))).andReturn("John");
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection,  statement, resultSet);

            runQuery(queryBase, gate);
            verify(gate, connection,  statement, resultSet);

        }

        protected abstract void runQuery(final AbstractQueryBase<Pair<Long, String>> queryBase, final DatabaseGate gate) throws SQLException;
    }

//    public void testWhereList() throws Exception {
//        new WhereScenario() {
//            @Override
//            protected void runQuery(final AbstractQueryExpression<Pair<Long, String>> queryExpression, final DatabaseGate gate) throws SQLException {
//                final List<Pair<Long, String>> list = queryExpression.list(gate);
//                assertEquals(1, list.size());
//                assertEquals(Pair.make(123L, "John"), list.get(0));
//            }
//        }.play();
//
//    }
//
//
//    public void testWhereScroll() throws Exception {
//        new WhereScenario() {
//            @Override
//            protected void runQuery(final AbstractQueryExpression<Pair<Long, String>> queryExpression, final DatabaseGate gate) throws SQLException {
//                queryExpression.scroll(gate, new Callback<Pair<Long, String>>() {
//                    int callCount = 0;
//
//                    @Override
//                    public boolean iterate(final Pair<Long, String> pair) {
//                        if (callCount++ != 0) {
//                            fail("One call expected, actually " + callCount);
//                        }
//                        assertEquals(Pair.make(123L, "John"), pair);
//                        return true;
//                    }
//                });
//            }
//        }.play();
//
//    }

    private static abstract class WhereScenario {
        public void play() throws Exception {
            final AbstractQueryExpression<Pair<Long, String>> queryExpression = person.id.pair(person.name).where(person.name.isNotNull());
            final String queryString = queryExpression.show(new GenericDialect());
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(new GenericDialect());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.getString(matches("C[0-9]"))).andReturn("John");
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection,  statement, resultSet);

            runQuery(queryExpression, gate);
            verify(gate, connection,  statement, resultSet);

        }

        protected abstract void runQuery(final AbstractQueryExpression<Pair<Long, String>> queryExpression, final DatabaseGate gate) throws SQLException;
    }


    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
    }

    private static Person person = new Person();

}
