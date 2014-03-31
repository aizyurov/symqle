package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractAggregateFunction;
import org.symqle.sql.AbstractValueExpression;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
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
public class ValueExpressionTest extends SqlTestCase {

    private AbstractValueExpression<Boolean> createValueExpression() {
        return person.name.eq(person.nickName).asValue();
    }

    public void testShow() throws Exception {
        final String sql = createValueExpression().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0", sql);
        final String sql2 = createValueExpression().showQuery(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testAdapt() throws Exception {
        final AbstractValueExpression<String> adaptor = AbstractValueExpression.adapt(person.name);
        final String sql = adaptor.showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0", sql);
        assertEquals(person.name.getMapper(), adaptor.getMapper());
    }

    public void testMap() throws Exception {
        final AbstractValueExpression<String> remapped = createValueExpression().map(CoreMappers.STRING);
        final String sql = remapped.showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0", sql);
        assertEquals(CoreMappers.STRING, remapped.getMapper());

    }

    public void testAll() throws Exception {
        final String sql = createValueExpression().selectAll().showQuery(new GenericDialect());
        assertSimilar("SELECT ALL T0.name = T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testDistinct() throws Exception {
        final String sql = createValueExpression().distinct().showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.name = T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = createValueExpression().where(person.married.asPredicate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 WHERE T0.married", sql);
    }

    public void testAsInValueList() throws Exception {
        final String sql = person.married.in(createValueExpression().asInValueList()).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.married IN(T0.name = T0.nick) AS C0 FROM person AS T0", sql);
    }

    public void testEq() throws Exception {
        final String sql = createValueExpression().eq(person.married).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) = T0.married AS C0 FROM person AS T0", sql);
    }

    public void testNe() throws Exception {
        final String sql = createValueExpression().ne(person.married).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) <> T0.married AS C0 FROM person AS T0", sql);
    }

    public void testGt() throws Exception {
        final String sql = createValueExpression().gt(person.married).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) > T0.married AS C0 FROM person AS T0", sql);
    }

    public void testGe() throws Exception {
        final String sql = createValueExpression().ge(person.married).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) >= T0.married AS C0 FROM person AS T0", sql);
    }

    public void testLt() throws Exception {
        final String sql = createValueExpression().lt(person.married).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) < T0.married AS C0 FROM person AS T0", sql);
    }

    public void testLe() throws Exception {
        final String sql = createValueExpression().le(person.married).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) <= T0.married AS C0 FROM person AS T0", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = createValueExpression().eq(true).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) = ? AS C0 FROM person AS T0", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = createValueExpression().ne(true).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) <> ? AS C0 FROM person AS T0", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = createValueExpression().gt(true).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) > ? AS C0 FROM person AS T0", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = createValueExpression().ge(true).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) >= ? AS C0 FROM person AS T0", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = createValueExpression().lt(true).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) < ? AS C0 FROM person AS T0", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = createValueExpression().le(true).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) <= ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(createValueExpression().asPredicate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick)", sql);
    }

    public void testPair() throws Exception {
        final String sql = createValueExpression().pair(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = createValueExpression().opposite().showQuery(new GenericDialect());
        assertSimilar("SELECT -(T0.name = T0.nick) AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = createValueExpression().cast("CHAR(10)").showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.name = T0.nick AS CHAR(10)) AS C0 FROM person AS T0", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(createValueExpression().in(employee.remote)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IN(SELECT T1.remote FROM employee AS T1)", sql);
    }


    public void testNotIn() throws Exception {
        final String sql = person.id.where(createValueExpression().notIn(employee.remote)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) NOT IN(SELECT T1.remote FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(createValueExpression().in(true, false)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IN(?, ?)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(createValueExpression().notIn(true, false)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) NOT IN(?, ?)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(createValueExpression().isNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(createValueExpression().isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IS NOT NULL", sql);
    }

    public void testAdd() throws Exception {
        final String sql = createValueExpression().add(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        final String sql = createValueExpression().sub(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        final String sql = createValueExpression().mult(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) * T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final String sql = createValueExpression().div(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) / T0.id AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        final String sql = createValueExpression().add(2).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithValue() throws Exception {
        final AbstractValueExpression<Boolean> valueExpression = createValueExpression();
        final String sql = valueExpression.add(valueExpression.param(true)).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithoutValue() throws Exception {
        final AbstractValueExpression<Boolean> valueExpression = createValueExpression();
        final String sql = valueExpression.add(valueExpression.param()).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) + ? AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        final String sql = createValueExpression().sub(2).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = createValueExpression().mult(2).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = createValueExpression().div(2).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final String sql = createValueExpression().concat(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) || T0.name AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = createValueExpression().concat(" test").showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) || ? AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = createValueExpression().substring(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.name = T0.nick) FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCharLength() throws Exception {
        final String sql = createValueExpression().charLength().showQuery(new GenericDialect());
        assertSimilar("SELECT CHAR_LENGTH((T0.name = T0.nick)) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = createValueExpression().substring(person.id, person.id.div(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.name = T0.nick) FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = createValueExpression().substring(2).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.name = T0.nick) FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = createValueExpression().substring(2, 5).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.name = T0.nick) FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        final String sql = createValueExpression().positionOf(person.married).showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.married IN(T0.name = T0.nick)) AS C0 FROM person AS T0", sql);
    }

    public void testParam() throws Exception {
        final String sql = createValueExpression().positionOf("e").showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN(T0.name = T0.nick)) AS C0 FROM person AS T0", sql);
    }


    public void testCollate() throws Exception {
        final String sql = createValueExpression().collate("latin1_general_ci").showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createValueExpression().forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = createValueExpression().forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testLimit() throws Exception {
        final String sql = createValueExpression().limit(20).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createValueExpression().limit(10, 20).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLabel() throws Exception {
        final Label l = new Label();
        final String sql = createValueExpression().label(l).orderBy(l).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 ORDER BY C0", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = createValueExpression().orderBy(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testAsSortSpecification() throws Exception {
        final String sql = person.name.orderBy(createValueExpression()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick", sql);
    }

    public void testAsc() throws Exception {
        final String sql = person.name.orderBy(createValueExpression().asc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick ASC", sql);
    }

    public void testDesc() throws Exception {
        final String sql = person.name.orderBy(createValueExpression().desc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick DESC", sql);
    }

    public void testNullsFirst() throws Exception {
        final String sql = person.name.orderBy(createValueExpression().nullsFirst()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick NULLS FIRST", sql);
    }

    public void testNullsLast() throws Exception {
        final String sql = person.name.orderBy(createValueExpression().nullsLast()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick NULLS LAST", sql);
    }

    public void testUnion() throws Exception {
        final String sql = createValueExpression().union(employee.remote).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 UNION SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = createValueExpression().unionAll(employee.remote).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 UNION ALL SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = createValueExpression().unionDistinct(employee.remote).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }


    public void testExcept() throws Exception {
        final String sql = createValueExpression().except(employee.remote).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 EXCEPT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = createValueExpression().exceptAll(employee.remote).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = createValueExpression().exceptDistinct(employee.remote).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = createValueExpression().intersect(employee.remote).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 INTERSECT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = createValueExpression().intersectAll(employee.remote).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = createValueExpression().intersectDistinct(employee.remote).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.name.eq(employee.name).asValue().exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.name = T0.name FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = employee.id.where(person.name.eq(employee.name).asValue().contains(true)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT T1.name = T0.name FROM person AS T1)", sql);
    }

    public void testAsInArgument() throws Exception {
        final String sql = employee.id.where(employee.remote.in(person.name.eq(employee.name).asValue())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.remote IN(SELECT T1.name = T0.name FROM person AS T1)", sql);
    }

    public void testAsInListArguments() throws Exception {
        final String sql = person.id.where(person.married.in(true, false)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.married IN(?, ?)", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.name.eq(employee.name).asValue().queryValue().where(person.name.eq("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.name = T1.name FROM employee AS T1) AS C0 FROM person AS T0 WHERE T0.name = ?", sql);
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(person.name.eq("John").asValue()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name = ? END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNull().then(DynamicParameter.create(CoreMappers.BOOLEAN, false)).orElse(person.name.eq("John").asValue()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NULL THEN ? ELSE T0.name = ? END AS C0 FROM person AS T0", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(createValueExpression().like(DynamicParameter.create(CoreMappers.STRING, "true"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(createValueExpression().notLike(DynamicParameter.create(CoreMappers.STRING, "true"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(createValueExpression().like("true")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(createValueExpression().notLike("true")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick NOT LIKE ?", sql);
    }
    
    public void testCount() throws Exception {
        final AbstractAggregateFunction<Integer> count = createValueExpression().count();
        final String sql = count.showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = createValueExpression().countDistinct().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testSum() throws Exception {
        final String sql = createValueExpression().sum().showQuery(new GenericDialect());
        assertSimilar("SELECT SUM(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testAvg() throws Exception {
        final String sql = createValueExpression().avg().showQuery(new GenericDialect());
        assertSimilar("SELECT AVG(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testMin() throws Exception {
        final String sql = createValueExpression().min().showQuery(new GenericDialect());
        assertSimilar("SELECT MIN(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testMax() throws Exception {
        final String sql = createValueExpression().max().showQuery(new GenericDialect());
        assertSimilar("SELECT MAX(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    

    public void testList() throws Exception {
        new Scenario(createValueExpression()) {
            @Override
            void use(AbstractValueExpression<Boolean> query, QueryEngine engine) throws SQLException {
                assertEquals(Arrays.asList(true), query.list(engine));
            }
        }.play();
//        final AbstractValueExpression<Boolean> valueExpression = person.name.eq(person.nickName).asValue();
//        final String queryString = valueExpression.showQuery(new GenericDialect());
//        final List<Boolean> expected = Arrays.asList(true);
//        final SqlParameters parameters = createMock(SqlParameters.class);
//        replay(parameters);
//        final List<Boolean> list = valueExpression.list(
//            new MockQueryEngine<Boolean>(new SqlContext(), expected, queryString, parameters));
//        assertEquals(expected, list);
//        verify(parameters);
    }

    public void testScroll() throws Exception {
        new Scenario(createValueExpression()) {
            @Override
            void use(AbstractValueExpression<Boolean> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Boolean>(true)));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(createValueExpression()) {
            @Override
            void use(AbstractValueExpression<Boolean> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine).scroll(new TestCallback<Boolean>(true)));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<Boolean, AbstractValueExpression<Boolean>> {
        protected Scenario(AbstractValueExpression<Boolean> query) {
            super(query);
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getBoolean()).andReturn(true);
        }
    }

    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<String> nickName = defineColumn(CoreMappers.STRING, "nick");
        public Column<Boolean> married = defineColumn(CoreMappers.BOOLEAN, "married");
    }

    private static class Employee extends TableOrView {
        @Override
        public String getTableName() {
            return "employee";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<Boolean> remote = defineColumn(CoreMappers.BOOLEAN, "remote");
    }

    private static Person person = new Person();
    private static Employee employee = new Employee();

}
