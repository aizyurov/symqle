package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.Mappers;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameter;
import org.symqle.common.SqlParameters;
import org.symqle.sql.AbstractNumericExpression;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class NumericExpressionTest extends SqlTestCase {

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.id.add(two).isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.id.add(two).isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? IS NOT NULL", sql);
    }


    public void testShow() throws Exception {
        final String sql = person.id.add(two).show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0", sql);
        final String sql2 = person.id.add(two).show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testMap() throws Exception {
        final String sql = person.id.add(two).map(Mappers.INTEGER).show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.id.add(two).selectAll().show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.id.add(two).distinct().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.add(two).where(person.id.eq(two)).show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 WHERE T0.id = ?", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.id.add(two).eq(person.id.add(0))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? = T0.id + ?", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.id.add(two).ne(person.id.add(0))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? <> T0.id + ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.id.add(two).gt(person.id.add(0))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? > T0.id + ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.id.add(two).ge(person.id.add(0))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? >= T0.id + ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.id.add(two).lt(person.id.add(0))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? < T0.id + ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.id.add(two).le(person.id.add(0))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? <= T0.id + ?", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.id.add(two).eq(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.id.add(two).ne(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.id.add(two).gt(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.id.add(two).ge(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.id.add(two).lt(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.id.add(two).le(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? <= ?", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.id.add(two).in(person2.id.add(two))).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id + ? IN(SELECT T2.id + ? FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.id.add(two).notIn(person2.id.add(0))).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id + ? NOT IN(SELECT T2.id + ? FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.id.add(two).in(10L, 12L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.id.add(two).notIn(10L, 12L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? NOT IN(?, ?)", sql);
   }

    public void testOrderBy() throws Exception {
        String sql = person.id.add(two).orderBy(person.id.add(two)).show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 ORDER BY T0.id + ?", sql);
    }

    public void testOrderAsc() throws Exception {
        String sql = person.id.add(two).orderAsc().show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 ORDER BY C0 ASC", sql);
    }

    public void testOrderDesc() throws Exception {
        String sql = person.id.add(two).orderDesc().show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 ORDER BY C0 DESC", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.id.add(two).nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id + ? NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.id.add(two).nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id + ? NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.id.add(two).desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id + ? DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.id.add(two).asc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id + ? ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = person.id.add(two).opposite().show(new GenericDialect());
        assertSimilar("SELECT -(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.id.add(two).cast("NUMBER").show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id + ? AS NUMBER) AS C0 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        String sql = person.id.add(two).add(person.id.add(two)).show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? +(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        String sql = person.id.add(two).pair(person.name).show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        String sql = person.id.add(2).add(3).show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.id.add(two).asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.id + ?)", sql);
    }

    public void testSub() throws Exception {
        String sql = person.id.add(two).sub(person.id.add(two)).show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? -(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        String sql = person.id.add(two).sub(2).show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.id.add(two).mult(person.id.add(two)).show(new GenericDialect());
        assertSimilar("SELECT(T0.id + ?) *(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.id.add(two).mult(2).show(new GenericDialect());
        assertSimilar("SELECT(T0.id + ?) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = person.id.add(two).div(person.id.add(two)).show(new GenericDialect());
        assertSimilar("SELECT(T0.id + ?) /(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.id.add(two).div(2).show(new GenericDialect());
        assertSimilar("SELECT(T0.id + ?) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.id.add(two).concat(person.id.add(two)).show(new GenericDialect());
        assertSimilar("SELECT(T0.id + ?) ||(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        String sql = person.id.add(two).substring(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.id + ?) FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        String sql = person.id.add(two).substring(person.id, person.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.id + ?) FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        String sql = person.id.add(two).substring(2).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.id + ?) FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        String sql = person.id.add(two).substring(2,5).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.id + ?) FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        String sql = person.id.add(two).positionOf(person.id).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.id IN(T0.id + ?)) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        String sql = person.id.add(two).positionOf("1").show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN(T0.id + ?)) AS C0 FROM person AS T0", sql);
    }



    public void testCollate() throws Exception {
        String sql = person.id.add(two).collate("latin1_general_ci").show(new GenericDialect());
        assertSimilar("SELECT(T0.id + ?) COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = person.id.add(two).concat(" id").show(new GenericDialect());
        assertSimilar("SELECT(T0.id + ?) || ? AS C0 FROM person AS T0", sql);
    }
    
    public void testUnion() throws Exception {
        final String sql = employee.id.where(person.id.add(2).union(person2.id.add(1).where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 UNION SELECT T2.id + ? FROM person AS T2 WHERE T2.name = T0.name)", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = employee.id.where(person.id.add(2).unionAll(person2.id.add(1).where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 UNION ALL SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = employee.id.where(person.id.add(2).unionDistinct(person2.id.add(1).where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 UNION DISTINCT SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExcept() throws Exception {
        final String sql = employee.id.where(person.id.add(2).except(person2.id.add(1).where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 EXCEPT SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = employee.id.where(person.id.add(2).exceptAll(person2.id.add(1).where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 EXCEPT ALL SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = employee.id.where(person.id.add(2).exceptDistinct(person2.id.add(1).where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 EXCEPT DISTINCT SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }


    public void testIntersect() throws Exception {
        final String sql = employee.id.where(person.id.add(2).intersect(person2.id.add(1).where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 INTERSECT SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = employee.id.where(person.id.add(2).intersectAll(person2.id.add(1).where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 INTERSECT ALL SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = employee.id.where(person.id.add(2).intersectDistinct(person2.id.add(1).where(person2.name.eq(employee.name))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 INTERSECT DISTINCT SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.id.add(2).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = employee.id.where(person.id.add(2).contains(3)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT T1.id + ? FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.add(2).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T1.id + ? AS C1 FROM person AS T1 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.add(2).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T1.id + ? AS C1 FROM person AS T1 FOR READ ONLY", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.id.add(2).queryValue().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id + ? FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(person.id.add(2)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.id + ? END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.id.add(1)).orElse(person.id.add(2)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.id + ? ELSE T0.id + ? END AS C0 FROM person AS T0", sql);
    }


    public void testLike() throws Exception {
        final String sql = person.id.where(person.id.add(1).like(DynamicParameter.create(Mappers.STRING, "12%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.id.add(1).notLike(DynamicParameter.create(Mappers.STRING, "12%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? NOT LIKE ?", sql);
    }

    public void  testLikeString() throws Exception {
        final String sql = person.id.where(person.id.add(1).like("12%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? LIKE ?", sql);
    }

    public void  testNotLikeString() throws Exception {
        final String sql = person.id.where(person.id.add(1).notLike("12%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? NOT LIKE ?", sql);
    }

    public void testCount() throws Exception {
        final String sql = person.id.add(1).count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.id.add(1).countDistinct().show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.id.add(1).sum().show(new GenericDialect());
        assertSimilar("SELECT SUM(T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.id.add(1).avg().show(new GenericDialect());
        assertSimilar("SELECT AVG(T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.id.add(1).min().show(new GenericDialect());
        assertSimilar("SELECT MIN(T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.id.add(1).max().show(new GenericDialect());
        assertSimilar("SELECT MAX(T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testList() throws Exception {
        final AbstractNumericExpression<Number> numericExpression = person.id.add(two);
        final String queryString = numericExpression.show(new GenericDialect());
        final List<Number> expected = Arrays.asList((Number)123L);
        final SqlParameters parameters = createMock(SqlParameters.class);
        final SqlParameter param =createMock(SqlParameter.class);
        expect(parameters.next()).andReturn(param);
        param.setLong(2L);
        replay(parameters, param);
        final List<Number> list = numericExpression.list(
            new MockQueryEngine<Number>(new SqlContext(), expected, queryString, parameters));
        assertEquals(expected.size(), list.size());
        assertEquals(expected.get(0).longValue(), list.get(0).longValue());
        verify(parameters, param);
    }

    public void testScroll() throws Exception {
        final AbstractNumericExpression<Number> numericExpression = person.id.add(two);
        final String queryString = numericExpression.show(new GenericDialect());
        final List<Number> expected = Arrays.asList((Number)123L);
        final SqlParameters parameters = createMock(SqlParameters.class);
        final SqlParameter param =createMock(SqlParameter.class);
        expect(parameters.next()).andReturn(param);
        param.setLong(2L);
        replay(parameters, param);
        int rows = numericExpression.scroll(
            new MockQueryEngine<Number>(new SqlContext(),
                    expected, queryString, parameters),
                new Callback<Number>() {
                    int callCount = 0;
                    @Override
                    public boolean iterate(final Number value) {
                        if (callCount++ > 0) {
                            fail("One row expected, actually " + callCount);
                        }
                        assertEquals(123, value.intValue());
                        return true;
                    }
                });
        assertEquals(1, rows);
        verify(parameters, param);
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }
    
    private static Person person = new Person();
    private static Person person2 = new Person();
    
    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Employee employee = new Employee();
    
    private static DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
