package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractCharacterFactor;
import org.simqle.sql.Column;
import org.simqle.sql.DatabaseGate;
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
public class CharacterFactorTest extends SqlTestCase {

    public void testShow() throws Exception {
        final String sql = characterFactor.show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testCast() throws Exception {
        final String sql = characterFactor.cast("CHAR(10)").show();
        assertSimilar("SELECT CAST(T1.name COLLATE latin1_general_ci AS CHAR(10)) AS C1 FROM person AS T1", sql);
    }

    public void testMap() throws Exception {
        final String sql = characterFactor.map(Mappers.STRING).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testPair() throws Exception {
        final String sql = characterFactor.pair(person.id).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1, T1.id AS C2 FROM person AS T1", sql);
    }

    public void testGenericDialect() throws Exception {
        final String sql = characterFactor.show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = characterFactor.forUpdate().show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = characterFactor.forReadOnly().show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 FOR READ ONLY", sql);
    }

    public void testAll() throws Exception {
        final String sql = characterFactor.all().show(GenericDialect.get());
        assertSimilar("SELECT ALL T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testDistinct() throws Exception {
        final String sql = characterFactor.distinct().show(GenericDialect.get());
        assertSimilar("SELECT DISTINCT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = characterFactor.orderBy(characterFactor).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY T1.name COLLATE latin1_general_ci", sql);
    }

    public void testWhereEq() throws Exception {
        final String sql = characterFactor.where(characterFactor.eq(person.name)).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci = T1.name", sql);
    }

    public void testNe() throws Exception {
        final String sql = characterFactor.where(characterFactor.ne(person.name)).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci <> T1.name", sql);
    }

    public void testGt() throws Exception {
        final String sql = characterFactor.where(characterFactor.gt(person.name)).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci > T1.name", sql);
    }

    public void testGe() throws Exception {
        final String sql = characterFactor.where(characterFactor.ge(person.name)).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci >= T1.name", sql);
    }

    public void testLt() throws Exception {
        final String sql = characterFactor.where(characterFactor.lt(person.name)).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci < T1.name", sql);
    }

    public void testLe() throws Exception {
        final String sql = characterFactor.where(characterFactor.le(person.name)).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci <= T1.name", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.eq("abc")).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.ne("abc")).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.gt("abc")).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.ge("abc")).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.lt("abc")).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.le("abc")).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci <= ?", sql);
    }

    public void testLike() throws Exception {
        final String sql = characterFactor.where(characterFactor.like(person.name)).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci LIKE T1.name", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = characterFactor.where(characterFactor.notLike(person.name)).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci NOT LIKE T1.name", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = characterFactor.where(characterFactor.like("abc")).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = characterFactor.where(characterFactor.notLike("abc")).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci NOT LIKE ?", sql);
    }

    public void testIn() throws Exception {
        final String sql = characterFactor.where(characterFactor.in(new Person().name)).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci IN(SELECT T2.name FROM person AS T2)", sql);
    }

    public void testInArgument() throws Exception {
        final Person selected = new Person();
        final String sql = selected.id.where(selected.name.in(characterFactor)).show(GenericDialect.get());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.name IN(SELECT T2.name COLLATE latin1_general_ci FROM person AS T2)", sql);
    }

    public void testQueryValue() throws Exception {
        final Person selected = new Person();
        final String sql = characterFactor.queryValue().pair(selected.id).show();
        assertSimilar("SELECT(SELECT T2.name COLLATE latin1_general_ci FROM person AS T2) AS C0, T1.id AS C1 FROM person AS T1", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = characterFactor.where(characterFactor.notIn(new Person().name)).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci NOT IN(SELECT T2.name FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        final String sql = characterFactor.where(characterFactor.in("abc", "def")).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci IN(?, ?)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = characterFactor.where(characterFactor.notIn("abc", "def")).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci NOT IN(?, ?)", sql);
    }

    public void testExists() throws Exception {
        final String sql = new Person().id.where(characterFactor.exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.name COLLATE latin1_general_ci FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = new Person().id.where(characterFactor.contains("abc")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.name COLLATE latin1_general_ci FROM person AS T1)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = characterFactor.where(characterFactor.isNull()).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = characterFactor.where(characterFactor.isNotNull()).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci IS NOT NULL", sql);
    }

    public void testConcat() throws Exception {
        final String sql = characterFactor.concat(characterFactor).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci || T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = characterFactor.concat("abc").show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci || ? AS C1 FROM person AS T1", sql);
    }

    public void testAdd() throws Exception {
        final String sql = characterFactor.add(person.id).show();
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) + T1.id AS C1 FROM person AS T1", sql);
    }

    public void testAddNumber() throws Exception {
        final String sql = characterFactor.add(2).show();
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) + ? AS C1 FROM person AS T1", sql);
    }

    public void testMult() throws Exception {
        final String sql = characterFactor.mult(person.id).show();
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) * T1.id AS C1 FROM person AS T1", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = characterFactor.mult(2).show();
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) * ? AS C1 FROM person AS T1", sql);
    }

    public void testSub() throws Exception {
        final String sql = characterFactor.sub(person.id).show();
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) - T1.id AS C1 FROM person AS T1", sql);
    }

    public void testSubNumber() throws Exception {
        final String sql = characterFactor.sub(2).show();
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) - ? AS C1 FROM person AS T1", sql);
    }

    public void testDiv() throws Exception {
        final String sql = characterFactor.div(person.id).show();
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) / T1.id AS C1 FROM person AS T1", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = characterFactor.div(2).show();
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) / ? AS C1 FROM person AS T1", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = characterFactor.opposite().show();
        assertSimilar("SELECT -(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testNullsFirst() throws Exception {
        final String sql = characterFactor.orderBy(characterFactor.nullsFirst()).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY T1.name COLLATE latin1_general_ci NULLS FIRST", sql);
    }

    public void testNullsLast() throws Exception {
        final String sql = characterFactor.orderBy(characterFactor.nullsLast()).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY T1.name COLLATE latin1_general_ci NULLS LAST", sql);
    }

    public void testAsc() throws Exception {
        final String sql = characterFactor.orderBy(characterFactor.asc()).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY T1.name COLLATE latin1_general_ci ASC", sql);
    }

    public void testDesc() throws Exception {
        final String sql = characterFactor.orderBy(characterFactor.desc()).show(GenericDialect.get());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY T1.name COLLATE latin1_general_ci DESC", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = characterFactor.unionAll(new Person().name).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 UNION ALL SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = characterFactor.unionDistinct(new Person().name).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 UNION DISTINCT SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testUnion() throws Exception {
        final String sql = characterFactor.union(new Person().name).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 UNION SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = characterFactor.exceptAll(new Person().name).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 EXCEPT ALL SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = characterFactor.exceptDistinct(new Person().name).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 EXCEPT DISTINCT SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testExcept() throws Exception {
        final String sql = characterFactor.except(new Person().name).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 EXCEPT SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = characterFactor.intersectAll(new Person().name).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 INTERSECT ALL SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = characterFactor.intersectDistinct(new Person().name).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 INTERSECT DISTINCT SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = characterFactor.intersect(new Person().name).show();
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 INTERSECT SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testCollate() throws Exception {
        final String sql = characterFactor.collate("latin1_general_cs").show();
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) COLLATE latin1_general_cs AS C1 FROM person AS T1", sql);
    }

    public void testCount() throws Exception {
        final String sql = characterFactor.count().show();
        assertSimilar("SELECT COUNT(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = characterFactor.countDistinct().show();
        assertSimilar("SELECT COUNT(DISTINCT T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testMin() throws Exception {
        final String sql = characterFactor.min().show();
        assertSimilar("SELECT MIN(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testMax() throws Exception {
        final String sql = characterFactor.max().show();
        assertSimilar("SELECT MAX(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testAvg() throws Exception {
        final String sql = characterFactor.avg().show();
        assertSimilar("SELECT AVG(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testSum() throws Exception {
        final String sql = characterFactor.sum().show();
        assertSimilar("SELECT SUM(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testElse() {
        final String sql = person.id.ge(100L).then(person.name).orElse(characterFactor).show();
        assertSimilar("SELECT CASE WHEN T1.id >= ? THEN T1.name ELSE T1.name COLLATE latin1_general_ci END AS C1 FROM person AS T1", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(characterFactor.booleanValue()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.name COLLATE latin1_general_ci)", sql);
    }


    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DatabaseGate gate, final AbstractCharacterFactor<String> characterFactor) throws SQLException {
                final List<String> list = characterFactor.list(gate);
                assertEquals(Arrays.asList("abc"), list);
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DatabaseGate gate, final AbstractCharacterFactor<String> characterFactor) throws SQLException {
                characterFactor.scroll(gate, new Callback<String>() {
                    private int callCount = 0;
                    @Override
                    public boolean iterate(final String s) {
                        assertEquals(0, callCount++);
                        assertEquals("abc", s);
                        return true;
                    }
                });
            }
        }.play();
    }






    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static final Person person = new Person();
    private static final AbstractCharacterFactor<String> characterFactor = person.name.collate("latin1_general_ci");


    private static abstract class Scenario {
        public void play() throws Exception {
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            final String queryString = characterFactor.show();
            expect(gate.getDialect()).andReturn(GenericDialect.get());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getString(matches("C[0-9]"))).andReturn("abc");
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection, statement, resultSet);

            runQuery(gate, characterFactor);
            verify(gate, connection, statement, resultSet);
        }

        protected abstract void runQuery(final DatabaseGate gate, final AbstractCharacterFactor<String> characterFactor) throws SQLException;
    }

}
