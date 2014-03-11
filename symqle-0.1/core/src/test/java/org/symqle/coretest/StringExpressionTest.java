package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

/**
 * @author lvovich
 */
public class StringExpressionTest extends SqlTestCase {

    private AbstractStringExpression<String> createStringExpression() {
        return numberSign.concat(person.id);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(createStringExpression().isNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(createStringExpression().isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id IS NOT NULL", sql);
    }


    public void testShow() throws Exception {
        final String sql = createStringExpression().showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0", sql);
        assertSimilar(sql, createStringExpression().showQuery(new GenericDialect()));
    }

    public void testAdapt() throws Exception {
        final AbstractStringExpression<Long> adaptor = AbstractStringExpression.adapt(person.id);
        final String sql = adaptor.showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0", sql);
        assertEquals(adaptor.getMapper(), person.id.getMapper());
    }

    public void testMap() throws Exception {
        final AbstractStringExpression<String> remapped = createStringExpression().map(CoreMappers.STRING);
        final String sql = remapped.showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0", sql);
        assertEquals(CoreMappers.STRING, remapped.getMapper());
    }

    public void testSelectAll() throws Exception {
        final String sql = createStringExpression().selectAll().showQuery(new GenericDialect());
        assertSimilar("SELECT ALL ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = createStringExpression().distinct().showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = createStringExpression().where(person.id.concat(numberSign).eq(numberSign)).showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 WHERE T0.id || ? = ?", sql);

    }

    public void testAsInValueList() throws Exception {
        final String sql = person.id.where(numberSign.in(createStringExpression().asInValueList())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(? || T0.id)", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(createStringExpression().eq(numberSign)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id = ?", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(createStringExpression().ne(numberSign)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <> ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(createStringExpression().gt(numberSign)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id > ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(createStringExpression().ge(numberSign)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id >= ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(createStringExpression().lt(numberSign)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id < ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(createStringExpression().le(numberSign)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <= ?", sql);
    }

    public void testEqValue() throws Exception {
        final AbstractStringExpression<String> stringExpression = createStringExpression();
        final AbstractPredicate predicate = stringExpression.eq("#12");
        final String sql = person.id.where(predicate).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(createStringExpression().ne("#12")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(createStringExpression().gt("#12")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(createStringExpression().ge("#12")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(createStringExpression().lt("#12")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(createStringExpression().le("#12")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <= ?", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(createStringExpression().in(numberSign.concat(person2.id))).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE ? || T1.id IN(SELECT ? || T2.id FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(createStringExpression().notIn(numberSign.concat(person2.id))).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE ? || T1.id NOT IN(SELECT ? || T2.id FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(createStringExpression().in("#123", "#124")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(createStringExpression().notIn("#123", "#124")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id NOT IN(?, ?)", sql);
   }

    public void testLabel() throws Exception {
        final Label l = new Label();
        String sql = createStringExpression().label(l).orderBy(l).showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 ORDER BY C0", sql);
    }

    public void testOrderBy() throws Exception {
        String sql = createStringExpression().orderBy(createStringExpression()).showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(createStringExpression().nullsFirst()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(createStringExpression().nullsLast()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(createStringExpression().desc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(createStringExpression().asc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = createStringExpression().opposite().showQuery(new GenericDialect());
        assertSimilar("SELECT -(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = createStringExpression().cast("CHAR(10)").showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(? || T0.id AS CHAR(10)) AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(createStringExpression().asPredicate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(? || T0.id)", sql);
    }

    public void testPair() throws Exception {
        String sql = createStringExpression().pair(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        String sql = createStringExpression().add(createStringExpression()).showQuery(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) +(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        String sql = createStringExpression().sub(createStringExpression()).showQuery(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) -(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = createStringExpression().mult(createStringExpression()).showQuery(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) *(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = createStringExpression().div(createStringExpression()).showQuery(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) /(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = createStringExpression().concat(createStringExpression()).showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id ||(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final AbstractStringExpression<String> substring = createStringExpression().substring(person.id);
        String sql = substring.showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? || T0.id FROM T0.id) AS C0 FROM person AS T0", sql);
        assertEquals(CoreMappers.STRING, substring.getMapper());
    }

    public void testSubstring2() throws Exception {
        final AbstractStringExpression<String> substring = createStringExpression().substring(person.id, person.id.div(2));
        String sql = substring.showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? || T0.id FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
        assertEquals(CoreMappers.STRING, substring.getMapper());
    }

    public void testSubstringParam() throws Exception {
        final AbstractStringExpression<String> substring = createStringExpression().substring(2);
        String sql = substring.showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? || T0.id FROM ?) AS C0 FROM person AS T0", sql);
        assertEquals(CoreMappers.STRING, substring.getMapper());
    }

    public void testSubstringParam2() throws Exception {
        final AbstractStringExpression<String> substring = createStringExpression().substring(2, 5);
        String sql = substring.showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? || T0.id FROM ? FOR ?) AS C0 FROM person AS T0", sql);
        assertEquals(CoreMappers.STRING, substring.getMapper());
    }

    public void testPosition() throws Exception {
        final AbstractNumericExpression<Integer> pos = createStringExpression().positionOf(person.id);
        String sql = pos.showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.id IN ? || T0.id) AS C0 FROM person AS T0", sql);
        assertEquals(CoreMappers.INTEGER, pos.getMapper());
    }

    public void testPositionParam() throws Exception {
        final AbstractNumericExpression<Integer> pos = createStringExpression().positionOf("A");
        String sql = pos.showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN ? || T0.id) AS C0 FROM person AS T0", sql);
        assertEquals(CoreMappers.INTEGER, pos.getMapper());
    }


    public void testAddNumber() throws Exception {
        String sql = createStringExpression().add(2).showQuery(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithValue() throws Exception {
        final AbstractStringExpression<String> stringExpression = createStringExpression();
        String sql = stringExpression.add(stringExpression.param("2")).showQuery(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithNoValue() throws Exception {
        final AbstractStringExpression<String> stringExpression = createStringExpression();
        String sql = stringExpression.add(stringExpression.param()).showQuery(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        String sql = createStringExpression().sub(2).showQuery(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = createStringExpression().mult(2).showQuery(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = createStringExpression().div(2).showQuery(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = createStringExpression().concat(" test").showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id || ? AS C0 FROM person AS T0", sql);
    }

    public void testCollate() throws Exception {
        String sql = createStringExpression().collate("latin1_general_ci").showQuery(new GenericDialect());
        // concat have less precedence, so parenthesized
        assertSimilar("SELECT(? || T0.id) COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.id.concat(" test").union(person2.id.concat(" test2")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 UNION SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.concat(" test").unionAll(person2.id.concat(" test2")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 UNION ALL SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.id.concat(" test").unionDistinct(person2.id.concat(" test2")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.id.concat(" test").except(person2.id.concat(" test2")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 EXCEPT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.concat(" test").exceptAll(person2.id.concat(" test2")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.id.concat(" test").exceptDistinct(person2.id.concat(" test2")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.id.concat(" test").intersect(person2.id.concat(" test2")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 INTERSECT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.concat(" test").intersectAll(person2.id.concat(" test2")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.concat(" test").intersectDistinct(person2.id.concat(" test2")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = person.id.where(person2.id.concat(" test").exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id || ? FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = person.id.where(person2.id.concat(" test").contains("my test")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.id || ? FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.concat(" test").forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.concat(" test").forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testLimit() throws Exception {
        final String sql = createStringExpression().limit(20).showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createStringExpression().limit(10, 20).showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testQueryValue() {
        final String sql = person.id.concat(" test").queryValue().orderBy(person2.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id || ? FROM person AS T0) AS C0 FROM person AS T1 ORDER BY T1.id", sql);
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(person.name.concat(" test")).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name || ? END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.name).orElse(person.name.concat(" test")).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name ELSE T0.name || ? END AS C0 FROM person AS T0", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").like(person.name.concat(" tes_"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? LIKE T0.name || ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").notLike(person.name.concat(" tes_"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? NOT LIKE T0.name || ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").like("J%tes_")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").notLike("J%tes_")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? NOT LIKE ?", sql);
    }
    
    public void testCount() throws Exception {
        final String sql = person.name.concat("test").count().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.name || ?) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.name.concat("test").countDistinct().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testSum() throws Exception {
        final String sql = person.name.concat("test").sum().showQuery(new GenericDialect());
        assertSimilar("SELECT SUM(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testAvg() throws Exception {
        final String sql = person.name.concat("test").avg().showQuery(new GenericDialect());
        assertSimilar("SELECT AVG(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testMin() throws Exception {
        final String sql = person.name.concat("test").min().showQuery(new GenericDialect());
        assertSimilar("SELECT MIN(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testMax() throws Exception {
        final String sql = person.name.concat("test").max().showQuery(new GenericDialect());
        assertSimilar("SELECT MAX(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    



    public void testList() throws Exception {
        new Scenario(createStringExpression()) {
            @Override
            void use(AbstractStringExpression<String> query, QueryEngine engine) throws SQLException {
                assertEquals(Arrays.asList("#1"), query.list(engine));
            }
        }.play();
    }


    public void testScroll() throws Exception {
        new Scenario(createStringExpression()) {
            @Override
            void use(AbstractStringExpression<String> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<String>("#1")));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(createStringExpression()) {
            @Override
            void use(AbstractStringExpression<String> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine).scroll(new TestCallback<String>("#1")));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<String, AbstractStringExpression<String>> {
        protected Scenario(AbstractStringExpression<String> query) {
            super(query);
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            final OutBox param1 =createMock(OutBox.class);
            expect(parameters.next()).andReturn(param1);
            param1.setString("#");
            return Collections.singletonList(param1);
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getString()).andReturn("#1");
        }
    }

    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }
    
    private static DynamicParameter<String> numberSign = DynamicParameter.create(CoreMappers.STRING, "#");

    private static Person person = new Person();
    private static Person person2 = new Person();


}
