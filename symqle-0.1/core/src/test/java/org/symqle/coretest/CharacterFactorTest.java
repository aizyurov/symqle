package org.symqle.coretest;

import org.symqle.common.InBox;
import org.symqle.common.Mappers;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractCharacterFactor;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Label;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.expect;

/**
 * @author lvovich
 */
public class CharacterFactorTest extends SqlTestCase {

    public void testShow() throws Exception {
        final String sql = characterFactor.show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testAdapt() throws Exception {
        final Column<String> column = person.name;
        final String sql1 = column.show(new GenericDialect());
        final AbstractCharacterFactor<String> adaptor = AbstractCharacterFactor.adapt(column);
        final String sql2 = adaptor.show(new GenericDialect());
        assertEquals(sql1, sql2);
        assertEquals(column.getMapper(), adaptor.getMapper());
    }

    public void testAsInValueList() throws Exception {
        final String sql = person.id.where(person.name.in(characterFactor.asInValueList())).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.name IN(T1.name COLLATE latin1_general_ci)", sql);
    }

    public void testLimit() throws Exception {
        final String sql = characterFactor.limit(10).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 FETCH FIRST 10 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = characterFactor.limit(10, 20).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testCast() throws Exception {
        final String sql = characterFactor.cast("CHAR(10)").show(new GenericDialect());
        assertSimilar("SELECT CAST(T1.name COLLATE latin1_general_ci AS CHAR(10)) AS C1 FROM person AS T1", sql);
    }

    public void testMap() throws Exception {
        final String sql = characterFactor.map(Mappers.STRING).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testPair() throws Exception {
        final String sql = characterFactor.pair(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1, T1.id AS C2 FROM person AS T1", sql);
    }

    public void testGenericDialect() throws Exception {
        final String sql = characterFactor.show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = characterFactor.forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = characterFactor.forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 FOR READ ONLY", sql);
    }

    public void testAll() throws Exception {
        final String sql = characterFactor.selectAll().show(new GenericDialect());
        assertSimilar("SELECT ALL T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testDistinct() throws Exception {
        final String sql = characterFactor.distinct().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testLabel() throws Exception {
        Label l = new Label();
        final String sql = characterFactor.label(l).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY C1", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = characterFactor.orderBy(characterFactor).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY T1.name COLLATE latin1_general_ci", sql);
    }

    public void testWhereEq() throws Exception {
        final String sql = characterFactor.where(characterFactor.eq(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci = T1.name", sql);
    }

    public void testNe() throws Exception {
        final String sql = characterFactor.where(characterFactor.ne(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci <> T1.name", sql);
    }

    public void testGt() throws Exception {
        final String sql = characterFactor.where(characterFactor.gt(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci > T1.name", sql);
    }

    public void testGe() throws Exception {
        final String sql = characterFactor.where(characterFactor.ge(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci >= T1.name", sql);
    }

    public void testLt() throws Exception {
        final String sql = characterFactor.where(characterFactor.lt(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci < T1.name", sql);
    }

    public void testLe() throws Exception {
        final String sql = characterFactor.where(characterFactor.le(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci <= T1.name", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.eq("abc")).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.ne("abc")).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.gt("abc")).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.ge("abc")).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.lt("abc")).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = characterFactor.where(characterFactor.le("abc")).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci <= ?", sql);
    }

    public void testLike() throws Exception {
        final String sql = characterFactor.where(characterFactor.like(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci LIKE T1.name", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = characterFactor.where(characterFactor.notLike(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci NOT LIKE T1.name", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = characterFactor.where(characterFactor.like("abc")).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = characterFactor.where(characterFactor.notLike("abc")).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci NOT LIKE ?", sql);
    }

    public void testIn() throws Exception {
        final String sql = characterFactor.where(characterFactor.in(new Person().name)).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci IN(SELECT T2.name FROM person AS T2)", sql);
    }

    public void testInArgument() throws Exception {
        final Person selected = new Person();
        final String sql = selected.id.where(selected.name.in(characterFactor)).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.name IN(SELECT T2.name COLLATE latin1_general_ci FROM person AS T2)", sql);
    }

    public void testQueryValue() throws Exception {
        final Person selected = new Person();
        final String sql = characterFactor.queryValue().pair(selected.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T2.name COLLATE latin1_general_ci FROM person AS T2) AS C0, T1.id AS C1 FROM person AS T1", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = characterFactor.where(characterFactor.notIn(new Person().name)).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci NOT IN(SELECT T2.name FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        final String sql = characterFactor.where(characterFactor.in("abc", "def")).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci IN(?, ?)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = characterFactor.where(characterFactor.notIn("abc", "def")).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci NOT IN(?, ?)", sql);
    }

    public void testExists() throws Exception {
        final String sql = new Person().id.where(characterFactor.exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.name COLLATE latin1_general_ci FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = new Person().id.where(characterFactor.contains("abc")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.name COLLATE latin1_general_ci FROM person AS T1)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = characterFactor.where(characterFactor.isNull()).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = characterFactor.where(characterFactor.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 WHERE T1.name COLLATE latin1_general_ci IS NOT NULL", sql);
    }

    public void testConcat() throws Exception {
        final String sql = characterFactor.concat(characterFactor).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci || T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = characterFactor.substring(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(T1.name COLLATE latin1_general_ci FROM T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = characterFactor.substring(2).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(T1.name COLLATE latin1_general_ci FROM ?) AS C1 FROM person AS T1", sql);
    }


    public void testSubstring2() throws Exception {
        final String sql = characterFactor.substring(person.id, person.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(T1.name COLLATE latin1_general_ci FROM T1.id FOR T1.id / ?) AS C1 FROM person AS T1", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = characterFactor.substring(2, 5).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(T1.name COLLATE latin1_general_ci FROM ? FOR ?) AS C1 FROM person AS T1", sql);
    }

    public void testPosition() throws Exception {
        final String sql = characterFactor.positionOf(person.name).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T1.name IN T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);

    }

    public void testPositionParam() throws Exception {
        final String sql = characterFactor.positionOf("A").show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);

    }

    public void testConcatString() throws Exception {
        final String sql = characterFactor.concat("abc").show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci || ? AS C1 FROM person AS T1", sql);
    }

    public void testAdd() throws Exception {
        final String sql = characterFactor.add(person.id).show(new GenericDialect());
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) + T1.id AS C1 FROM person AS T1", sql);
    }

    public void testAddNumber() throws Exception {
        final String sql = characterFactor.add(2).show(new GenericDialect());
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) + ? AS C1 FROM person AS T1", sql);
    }

    public void testMult() throws Exception {
        final String sql = characterFactor.mult(person.id).show(new GenericDialect());
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) * T1.id AS C1 FROM person AS T1", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = characterFactor.mult(2).show(new GenericDialect());
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) * ? AS C1 FROM person AS T1", sql);
    }

    public void testSub() throws Exception {
        final String sql = characterFactor.sub(person.id).show(new GenericDialect());
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) - T1.id AS C1 FROM person AS T1", sql);
    }

    public void testSubNumber() throws Exception {
        final String sql = characterFactor.sub(2).show(new GenericDialect());
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) - ? AS C1 FROM person AS T1", sql);
    }

    public void testDiv() throws Exception {
        final String sql = characterFactor.div(person.id).show(new GenericDialect());
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) / T1.id AS C1 FROM person AS T1", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = characterFactor.div(2).show(new GenericDialect());
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) / ? AS C1 FROM person AS T1", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = characterFactor.opposite().show(new GenericDialect());
        assertSimilar("SELECT -(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testNullsFirst() throws Exception {
        final String sql = characterFactor.orderBy(characterFactor.nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY T1.name COLLATE latin1_general_ci NULLS FIRST", sql);
    }

    public void testNullsLast() throws Exception {
        final String sql = characterFactor.orderBy(characterFactor.nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY T1.name COLLATE latin1_general_ci NULLS LAST", sql);
    }

    public void testAsc() throws Exception {
        final String sql = characterFactor.orderBy(characterFactor.asc()).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY T1.name COLLATE latin1_general_ci ASC", sql);
    }

    public void testDesc() throws Exception {
        final String sql = characterFactor.orderBy(characterFactor.desc()).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 ORDER BY T1.name COLLATE latin1_general_ci DESC", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = characterFactor.unionAll(new Person().name).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 UNION ALL SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = characterFactor.unionDistinct(new Person().name).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 UNION DISTINCT SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testUnion() throws Exception {
        final String sql = characterFactor.union(new Person().name).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 UNION SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = characterFactor.exceptAll(new Person().name).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 EXCEPT ALL SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = characterFactor.exceptDistinct(new Person().name).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 EXCEPT DISTINCT SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testExcept() throws Exception {
        final String sql = characterFactor.except(new Person().name).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 EXCEPT SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = characterFactor.intersectAll(new Person().name).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 INTERSECT ALL SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = characterFactor.intersectDistinct(new Person().name).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 INTERSECT DISTINCT SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = characterFactor.intersect(new Person().name).show(new GenericDialect());
        assertSimilar("SELECT T1.name COLLATE latin1_general_ci AS C1 FROM person AS T1 INTERSECT SELECT T2.name AS C1 FROM person AS T2", sql);
    }

    public void testCollate() throws Exception {
        final String sql = characterFactor.collate("latin1_general_cs").show(new GenericDialect());
        assertSimilar("SELECT(T1.name COLLATE latin1_general_ci) COLLATE latin1_general_cs AS C1 FROM person AS T1", sql);
    }

    public void testCount() throws Exception {
        final String sql = characterFactor.count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = characterFactor.countDistinct().show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testMin() throws Exception {
        final String sql = characterFactor.min().show(new GenericDialect());
        assertSimilar("SELECT MIN(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testMax() throws Exception {
        final String sql = characterFactor.max().show(new GenericDialect());
        assertSimilar("SELECT MAX(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testAvg() throws Exception {
        final String sql = characterFactor.avg().show(new GenericDialect());
        assertSimilar("SELECT AVG(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testSum() throws Exception {
        final String sql = characterFactor.sum().show(new GenericDialect());
        assertSimilar("SELECT SUM(T1.name COLLATE latin1_general_ci) AS C1 FROM person AS T1", sql);
    }

    public void testElse() {
        final String sql = person.id.ge(100L).then(person.name).orElse(characterFactor).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T1.id >= ? THEN T1.name ELSE T1.name COLLATE latin1_general_ci END AS C1 FROM person AS T1", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(characterFactor.asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.name COLLATE latin1_general_ci)", sql);
    }


    public void testList() throws Exception {
        new Scenario(characterFactor) {
            @Override
            void use(AbstractCharacterFactor<String> query, QueryEngine engine) throws SQLException {
                final List<String> expected = Arrays.asList("abc");
                assertEquals(expected, query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(characterFactor) {
            @Override
            void use(AbstractCharacterFactor<String> query, QueryEngine engine) throws SQLException {
                int rows = query.scroll(engine,  new TestCallback<String>("abc"));
                assertEquals(1, rows);
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<String, AbstractCharacterFactor<String>> {

        private Scenario(AbstractCharacterFactor<String> query) {
            super(query);
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getString()).andReturn("abc");
        }
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

}
