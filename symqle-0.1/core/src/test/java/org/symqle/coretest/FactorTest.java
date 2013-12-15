package org.symqle.coretest;

import org.symqle.common.*;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.*;


/**
 * @author lvovich
 */
public class FactorTest extends SqlTestCase {

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.id.opposite().isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.id.opposite().isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id IS NOT NULL", sql);
    }


    public void testShow() throws Exception {
        final String sql = person.id.opposite().show(new GenericDialect());
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0", sql);
        final String sql2 = person.id.opposite().show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testLimit() throws Exception {
        final String sql = person.id.opposite().limit(10).show(new GenericDialect());
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 FETCH FIRST 10 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = person.id.opposite().limit(10, 20).show(new GenericDialect());
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testAsSublist() throws Exception {
        final String sql = person.id.opposite().queryValue().show(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT(SELECT - T0.id FROM person AS T0) AS C0 FROM dual AS T1", sql);
    }

    public void testMap() throws Exception {
        final String sql = person.id.opposite().map(Mappers.DOUBLE).show(new GenericDialect());
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.id.opposite().selectAll().show(new GenericDialect());
        assertSimilar("SELECT ALL - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.id.opposite().distinct().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.opposite().where(person.smart.asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 WHERE T0.smart", sql);

    }

    public void testPair() throws Exception {
        final String sql = person.id.opposite().pair(person.name).show(new GenericDialect());
        assertSimilar("SELECT - T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.id.opposite().eq(person.id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id = T0.id", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.id.opposite().ne(person.id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id <> T0.id", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.id.opposite().gt(person.id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id > T0.id", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.id.opposite().ge(person.id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id >= T0.id", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.id.opposite().lt(person.id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id < T0.id", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.id.opposite().le(person.id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id <= T0.id", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.id.opposite().eq(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.id.opposite().ne(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.id.opposite().gt(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.id.opposite().ge(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.id.opposite().lt(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.id.opposite().le(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id <= ?", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.id.opposite().in(person2.id.opposite())).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE - T1.id IN(SELECT - T2.id FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.id.opposite().notIn(person2.id)).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE - T1.id NOT IN(SELECT T2.id FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.id.opposite().in(-1L, 1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.id.opposite().notIn(-1L, 1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id NOT IN(?, ?)", sql);
   }

    public void testOpposite() throws Exception {
        final String sql = person.id.opposite().opposite().show(new GenericDialect());
        assertSimilar("SELECT -(- T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.id.opposite().cast("NUMBER").show(new GenericDialect());
        assertSimilar("SELECT CAST(- T0.id AS NUMBER) AS C0 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        String sql = person.id.opposite().add(person.id.opposite()).show(new GenericDialect());
        assertSimilar("SELECT - T0.id + - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        String sql = person.id.opposite().add(2).show(new GenericDialect());
        assertSimilar("SELECT - T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.id.opposite().asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(- T0.id)", sql);
    }

    public void testSub() throws Exception {
        String sql = person.id.opposite().sub(person.id.opposite()).show(new GenericDialect());
        assertSimilar("SELECT - T0.id - - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        String sql = person.id.opposite().sub(2).show(new GenericDialect());
        assertSimilar("SELECT - T0.id - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.id.opposite().mult(person.id.opposite()).show(new GenericDialect());
        assertSimilar("SELECT - T0.id * - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.id.opposite().mult(-2).show(new GenericDialect());
        assertSimilar("SELECT - T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = person.id.opposite().div(person.id.opposite()).show(new GenericDialect());
        assertSimilar("SELECT - T0.id / - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.id.opposite().div(2).show(new GenericDialect());
        assertSimilar("SELECT - T0.id / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.id.opposite().concat(person.id.opposite()).show(new GenericDialect());
        assertSimilar("SELECT(- T0.id) ||(- T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        String sql = person.id.opposite().substring(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((- T0.id) FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        String sql = person.id.opposite().substring(person.id, person.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((- T0.id) FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        String sql = person.id.opposite().substring(2).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((- T0.id) FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        String sql = person.id.opposite().substring(2,5).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((- T0.id) FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        String sql = person.id.opposite().positionOf(person.id).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.id IN(- T0.id)) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        String sql = person.id.opposite().positionOf("-").show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN(- T0.id)) AS C0 FROM person AS T0", sql);
    }

    public void testCollate() throws Exception {
        String sql = person.id.opposite().collate("latin1_general_ci").show(new GenericDialect());
        assertSimilar("SELECT(- T0.id) COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = person.id.opposite().concat(" id").show(new GenericDialect());
        assertSimilar("SELECT(- T0.id) || ? AS C0 FROM person AS T0", sql);
    }
    
    public void testOrderBy() throws Exception {
        String sql = person.id.opposite().orderBy(person.id.opposite()).show(new GenericDialect());
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 ORDER BY - T0.id", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.id.opposite().nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY - T0.id NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.id.opposite().nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY - T0.id NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.id.opposite().desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY - T0.id DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.id.opposite().asc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY - T0.id ASC", sql);
    }

    public void testUnion() throws Exception {
        final String sql = employee.id.where(person.id.opposite().union(person2.id.where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1 UNION SELECT T2.id FROM person AS T2 WHERE T2.name = T0.name)", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = employee.id.where(person.id.opposite().unionAll(person2.id.where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1 UNION ALL SELECT T2.id FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = employee.id.where(person.id.opposite().unionDistinct(person2.id.where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1 UNION DISTINCT SELECT T2.id FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExcept() throws Exception {
        final String sql = employee.id.where(person.id.opposite().except(person2.id.where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1 EXCEPT SELECT T2.id FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = employee.id.where(person.id.opposite().exceptAll(person2.id.where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1 EXCEPT ALL SELECT T2.id FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = employee.id.where(person.id.opposite().exceptDistinct(person2.id.where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1 EXCEPT DISTINCT SELECT T2.id FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }


    public void testIntersect() throws Exception {
        final String sql = employee.id.where(person.id.opposite().intersect(person2.id.where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1 INTERSECT SELECT T2.id FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = employee.id.where(person.id.opposite().intersectAll(person2.id.where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1 INTERSECT ALL SELECT T2.id FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = employee.id.where(person.id.opposite().intersectDistinct(person2.id.where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1 INTERSECT DISTINCT SELECT T2.id FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.id.opposite().exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = employee.id.where(person.id.opposite().contains(-1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT - T1.id FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.opposite().forUpdate().show(new GenericDialect());
        assertSimilar("SELECT - T1.id AS C1 FROM person AS T1 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.opposite().forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT - T1.id AS C1 FROM person AS T1 FOR READ ONLY", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.id.opposite().queryValue().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT(SELECT - T0.id FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(person.id.opposite()).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN - T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.id).orElse(person.id.opposite()).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.id ELSE - T0.id END AS C0 FROM person AS T0", sql);
    }


    public void testLike() throws Exception {
        final String sql = person.id.where(person.id.opposite().like(DynamicParameter.create(Mappers.STRING, "12%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.id.opposite().notLike(DynamicParameter.create(Mappers.STRING, "12%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id NOT LIKE ?", sql);
    }

    public void  testLikeString() throws Exception {
        final String sql = person.id.where(person.id.opposite().like("12%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id LIKE ?", sql);
    }

    public void  testNotLikeString() throws Exception {
        final String sql = person.id.where(person.id.opposite().notLike("12%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id NOT LIKE ?", sql);
    }

    public void testCount() throws Exception {
        final String sql = person.id.opposite().count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(- T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.id.opposite().countDistinct().show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT - T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.id.opposite().avg().show(new GenericDialect());
        assertSimilar("SELECT AVG(- T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.id.opposite().sum().show(new GenericDialect());
        assertSimilar("SELECT SUM(- T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.id.opposite().min().show(new GenericDialect());
        assertSimilar("SELECT MIN(- T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.id.opposite().max().show(new GenericDialect());
        assertSimilar("SELECT MAX(- T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testList() throws Exception {
        new Scenario(person.id.opposite()) {
            @Override
            void use(AbstractFactor<Long> query, QueryEngine engine) throws SQLException {
                final List<Long> expected = Arrays.asList(-123L);
                assertEquals(expected, query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(person.id.opposite()) {
            @Override
            void use(AbstractFactor<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Long>(-123L)));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<Long, AbstractFactor<Long>> {

        private Scenario(AbstractFactor<Long> query) {
            super(query);
        }

        @Override
        List<SqlParameter> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(Element element) throws SQLException {
            expect(element.getLong()).andReturn(-123L);
        }
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Boolean> smart = defineColumn(Mappers.BOOLEAN, "smart");
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();
    private static Person person2 = new Person();

    private static Employee employee = new Employee();


}
