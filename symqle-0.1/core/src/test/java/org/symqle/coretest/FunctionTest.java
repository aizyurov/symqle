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
public class FunctionTest extends SqlTestCase {

    private AbstractRoutineInvocation<String> currentUser() {
        return SqlFunction.create("user", CoreMappers.STRING).apply();
    }

    private static AbstractRoutineInvocation<Long> abs(ValueExpression<Long> e) {

        return SqlFunction.create("abs", CoreMappers.LONG).apply(e);
    }

    public void testShow() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0", abs(col).showQuery(new GenericDialect()));
        assertSimilar(abs(col).showQuery(new GenericDialect()), abs(col).showQuery(new GenericDialect()));
    }

    public void testAdapt() throws Exception {
        final AbstractRoutineInvocation<Long> adaptor = AbstractRoutineInvocation.adapt(abs(person.id));
        assertEquals(adaptor.showQuery(new GenericDialect()), abs(person.id).showQuery(new GenericDialect()));
        assertEquals(adaptor.getMapper(), abs(person.id).getMapper());
    }

    public void testLimit() throws Exception {
        final String sql = abs(person.id).limit(20).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = abs(person.id).limit(10, 20).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testMap() throws Exception {
        final Column<Long> col = person.id;
        final AbstractValueExpressionPrimary<Long> remapped = abs(col).map(CoreMappers.LONG);
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0", remapped.showQuery(new GenericDialect()));
        assertSimilar(abs(col).showQuery(new GenericDialect()), abs(col).showQuery(new GenericDialect()));
        assertEquals(CoreMappers.LONG, remapped.getMapper());
    }

    public void testSelectAll() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT ALL abs(T0.id) AS C0 FROM person AS T0", abs(col).selectAll().showQuery(new GenericDialect()));

    }

    public void testSelectDistinct() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT DISTINCT abs(T0.id) AS C0 FROM person AS T0", abs(col).distinct().showQuery(new GenericDialect()));
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = SqlFunction.create("abs", CoreMappers.LONG) .apply(abs(person.id)).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(abs(T0.id)) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(abs(id).asPredicate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id)", sql);
    }

    public void testEq() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).eq(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) = T0.age", sql);
    }

    public void testNe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).ne(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).gt(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) > T0.age", sql);
    }

    public void testGe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).ge(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).lt(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) < T0.age", sql);
    }

    public void testLe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).le(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <= T0.age", sql);
    }

    public void testEqValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).eq(0L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).ne(0L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).gt(0L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).ge(0L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).lt(0L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).le(0L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <= ?", sql);
    }

    public void testInAll() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id).in(id2.selectAll())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id).in(abs(id2))).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE abs(T1.id) IN(SELECT abs(T2.id) FROM employee AS T2)", sql);
    }

    public void testInValueList() throws Exception {
        final String sql = person.id.where(person.id.in(abs(person.id).asInValueList())).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id IN(abs(T1.id))", sql);
    }


    public void testContains() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id2).contains(1L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE ? IN(SELECT abs(T2.id) FROM employee AS T2)", sql);
    }

    public void testNotInAll() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id).notIn(id2.selectAll())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) NOT IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(abs(id).in(1L, 2L, 3L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(abs(id).notIn(1L, 2L, 3L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(abs(age).isNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.age) IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(abs(age).isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.age) IS NOT NULL", sql);
   }

    public void testLabel() throws Exception {
        final Column<Long> id  =  person.id;
        final Label l = new Label();
        String sql = abs(id).label(l).orderBy(l).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 ORDER BY C0", sql);
    }

    public void testOrderBy() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).orderBy(abs(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 ORDER BY abs(T0.age)", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).nullsFirst()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).nullsLast()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).desc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).asc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) ASC", sql);
    }

    public void testOpposite() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).opposite().showQuery(new GenericDialect());
        assertSimilar("SELECT - abs(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).cast("NUMBER").showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(abs(T0.id) AS NUMBER) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).pair(age).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0, T0.age AS C1 FROM person AS T0", sql);
    }

    public void testNoArgFunction() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = currentUser().pair(id).showQuery(new GenericDialect());
        assertSimilar("SELECT user() AS C0, T0.id AS C1 FROM person AS T0", sql);
        assertEquals(CoreMappers.STRING, currentUser().getMapper());
    }

    public void testAdd() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).add(age).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) + T0.age AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).add(1).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithValue() throws Exception {
        final Column<Long> id  =  person.id;
        final AbstractRoutineInvocation<Long> abs = abs(id);
        String sql = abs.add(abs.param(1L)).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithoutValue() throws Exception {
        final Column<Long> id  =  person.id;
        final AbstractRoutineInvocation<Long> abs = abs(id);
        String sql = abs.add(abs.param()).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).sub(age).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) - T0.age AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).sub(2).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).mult(age).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).mult(2).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).div(age).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) / T0.age AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).div(3).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).concat(age).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) || T0.age AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        String sql = abs(person.id).substring(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(abs(T0.id) FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        String sql = abs(person.id).substring(person.id, person.id.div(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(abs(T0.id) FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        String sql = abs(person.id).substring(2).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(abs(T0.id) FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        String sql = abs(person.id).substring(2,5).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(abs(T0.id) FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        String sql = abs(person.id).positionOf(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.id IN abs(T0.id)) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        String sql = abs(person.id).positionOf("1").showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN abs(T0.id)) AS C0 FROM person AS T0", sql);
    }




    public void testConcatString() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).concat(" id").showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) || ? AS C0 FROM person AS T0", sql);
    }

    public void testCollate() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).collate("latin1_general_ci").showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = abs(person.id).union(person2.id).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 UNION SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = abs(person.id).unionAll(person2.id).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 UNION ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = abs(person.id).unionDistinct(person2.id).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = abs(person.id).except(person2.id).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 EXCEPT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = abs(person.id).exceptAll(person2.id).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = abs(person.id).exceptDistinct(person2.id).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = abs(person.id).intersect(person2.id).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 INTERSECT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = abs(person.id).intersectAll(person2.id).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = abs(person.id).intersectDistinct(person2.id).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = abs(person.id).forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = abs(person.id).forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testExists() throws Exception {
        final String sql = person2.id.where(abs(person.id).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT abs(T1.id) FROM person AS T1)", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = abs(person.id).queryValue().orderBy(person2.age).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT abs(T0.id) FROM person AS T0) AS C0 FROM person AS T1 ORDER BY T1.age", sql);

    }

    public void testLike() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", CoreMappers.STRING).apply(person.name).like(DynamicParameter.create(CoreMappers.STRING, "J%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", CoreMappers.STRING).apply(person.name).notLike(DynamicParameter.create(CoreMappers.STRING, "J%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", CoreMappers.STRING).apply(person.name).like("J%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", CoreMappers.STRING).apply(person.name).notLike("J%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) NOT LIKE ?", sql);
    }

    public void testCount() throws Exception {
        final String sql = abs(person.id).count().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = abs(person.id).countDistinct().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testAvg() throws Exception {
        final String sql = abs(person.id).avg().showQuery(new GenericDialect());
        assertSimilar("SELECT AVG(abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testSum() throws Exception {
        final String sql = abs(person.id).sum().showQuery(new GenericDialect());
        assertSimilar("SELECT SUM(abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testMin() throws Exception {
        final String sql = abs(person.id).min().showQuery(new GenericDialect());
        assertSimilar("SELECT MIN(abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testMax() throws Exception {
        final String sql = abs(person.id).max().showQuery(new GenericDialect());
        assertSimilar("SELECT MAX(abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testList() throws Exception {
        new Scenario(abs(person.id)) {
            @Override
            void use(AbstractRoutineInvocation<Long> query, QueryEngine engine) throws SQLException {
                List<Long> expected = Arrays.asList(123L);
                assertEquals(expected, query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(abs(person.id)) {
            @Override
            void use(AbstractRoutineInvocation<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Long>(123L)));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(abs(person.id)) {
            @Override
            void use(AbstractRoutineInvocation<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine).scroll(new TestCallback<Long>(123L)));
            }
        }.play();
    }

    public void testCharLength() throws Exception {
        final String sql = currentUser().charLength().showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT CHAR_LENGTH(user()) AS C0 FROM dual AS T0", sql);
    }

    private static abstract class Scenario extends AbstractQueryScenario<Long, AbstractRoutineInvocation<Long>> {

        private Scenario(AbstractRoutineInvocation<Long> query) {
            super(query);
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getLong()).andReturn(123L);
        }
    }

    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<Long> age = defineColumn(CoreMappers.LONG, "age");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<Long> parentId = defineColumn(CoreMappers.LONG, "parent_id");
    }

    private static class Employee extends TableOrView {
        @Override
        public String getTableName() {
            return "employee";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
    }

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

}
