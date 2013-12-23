package org.symqle.coretest;

import org.symqle.common.*;
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
        return SqlFunction.create("user", Mappers.STRING).apply();
    }

    private static AbstractRoutineInvocation<Long> abs(ValueExpression<Long> e) {

        return SqlFunction.create("abs", Mappers.LONG).apply(e);
    }

    public void testShow() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0", abs(col).show(new GenericDialect()));
        assertSimilar(abs(col).show(new GenericDialect()), abs(col).show(new GenericDialect()));
    }

    public void testAdapt() throws Exception {
        final AbstractRoutineInvocation<Long> adaptor = AbstractRoutineInvocation.adapt(abs(person.id));
        assertEquals(adaptor.show(new GenericDialect()), abs(person.id).show(new GenericDialect()));
        assertEquals(adaptor.getMapper(), abs(person.id).getMapper());
    }

    public void testLimit() throws Exception {
        final String sql = abs(person.id).limit(20).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = abs(person.id).limit(10, 20).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testMap() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0", abs(col).map(Mappers.LONG).show(new GenericDialect()));
        assertSimilar(abs(col).show(new GenericDialect()), abs(col).show(new GenericDialect()));
    }

    public void testSelectAll() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT ALL abs(T0.id) AS C0 FROM person AS T0", abs(col).selectAll().show(new GenericDialect()));

    }

    public void testSelectDistinct() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT DISTINCT abs(T0.id) AS C0 FROM person AS T0", abs(col).distinct().show(new GenericDialect()));
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = SqlFunction.create("abs", Mappers.LONG) .apply(abs(person.id)).show(new GenericDialect());
        assertSimilar("SELECT abs(abs(T0.id)) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(abs(id).asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id)", sql);
    }

    public void testEq() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).eq(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) = T0.age", sql);
    }

    public void testNe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).ne(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).gt(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) > T0.age", sql);
    }

    public void testGe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).ge(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).lt(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) < T0.age", sql);
    }

    public void testLe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).le(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <= T0.age", sql);
    }

    public void testEqValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).eq(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).ne(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).gt(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).ge(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).lt(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).le(0L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <= ?", sql);
    }

    public void testInAll() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id).in(id2.selectAll())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id).in(abs(id2))).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE abs(T1.id) IN(SELECT abs(T2.id) FROM employee AS T2)", sql);
    }

    public void testContains() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id2).contains(1L)).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE ? IN(SELECT abs(T2.id) FROM employee AS T2)", sql);
    }

    public void testNotInAll() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id).notIn(id2.selectAll())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) NOT IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(abs(id).in(1L, 2L, 3L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(abs(id).notIn(1L, 2L, 3L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(abs(age).isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.age) IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(abs(age).isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.age) IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).orderBy(abs(age)).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 ORDER BY abs(T0.age)", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).asc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) ASC", sql);
    }

    public void testOpposite() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).opposite().show(new GenericDialect());
        assertSimilar("SELECT - abs(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).cast("NUMBER").show(new GenericDialect());
        assertSimilar("SELECT CAST(abs(T0.id) AS NUMBER) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).pair(age).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0, T0.age AS C1 FROM person AS T0", sql);
    }

    public void testNoArgFunction() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = currentUser().pair(id).show(new GenericDialect());
        assertSimilar("SELECT user() AS C0, T0.id AS C1 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).add(age).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) + T0.age AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).add(1).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).sub(age).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) - T0.age AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).sub(2).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).mult(age).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).mult(2).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).div(age).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) / T0.age AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).div(3).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).concat(age).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) || T0.age AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        String sql = abs(person.id).substring(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(abs(T0.id) FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        String sql = abs(person.id).substring(person.id, person.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(abs(T0.id) FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        String sql = abs(person.id).substring(2).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(abs(T0.id) FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        String sql = abs(person.id).substring(2,5).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(abs(T0.id) FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        String sql = abs(person.id).positionOf(person.id).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.id IN abs(T0.id)) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        String sql = abs(person.id).positionOf("1").show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN abs(T0.id)) AS C0 FROM person AS T0", sql);
    }




    public void testConcatString() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).concat(" id").show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) || ? AS C0 FROM person AS T0", sql);
    }

    public void testCollate() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).collate("latin1_general_ci").show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = abs(person.id).union(person2.id).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 UNION SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = abs(person.id).unionAll(person2.id).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 UNION ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = abs(person.id).unionDistinct(person2.id).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = abs(person.id).except(person2.id).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 EXCEPT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = abs(person.id).exceptAll(person2.id).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = abs(person.id).exceptDistinct(person2.id).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = abs(person.id).intersect(person2.id).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 INTERSECT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = abs(person.id).intersectAll(person2.id).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = abs(person.id).intersectDistinct(person2.id).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = abs(person.id).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = abs(person.id).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testExists() throws Exception {
        final String sql = person2.id.where(abs(person.id).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT abs(T1.id) FROM person AS T1)", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = abs(person.id).queryValue().orderBy(person2.age).show(new GenericDialect());
        assertSimilar("SELECT(SELECT abs(T0.id) FROM person AS T0) AS C0 FROM person AS T1 ORDER BY T1.age", sql);

    }

    public void testLike() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", Mappers.STRING).apply(person.name).like(DynamicParameter.create(Mappers.STRING, "J%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", Mappers.STRING).apply(person.name).notLike(DynamicParameter.create(Mappers.STRING, "J%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", Mappers.STRING).apply(person.name).like("J%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", Mappers.STRING).apply(person.name).notLike("J%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) NOT LIKE ?", sql);
    }

    public void testCount() throws Exception {
        final String sql = abs(person.id).count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = abs(person.id).countDistinct().show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testAvg() throws Exception {
        final String sql = abs(person.id).avg().show(new GenericDialect());
        assertSimilar("SELECT AVG(abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testSum() throws Exception {
        final String sql = abs(person.id).sum().show(new GenericDialect());
        assertSimilar("SELECT SUM(abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testMin() throws Exception {
        final String sql = abs(person.id).min().show(new GenericDialect());
        assertSimilar("SELECT MIN(abs(T1.id)) AS C1 FROM person AS T1", sql);
    }

    public void testMax() throws Exception {
        final String sql = abs(person.id).max().show(new GenericDialect());
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

    private static abstract class Scenario extends AbstractQueryScenario<Long, AbstractRoutineInvocation<Long>> {

        private Scenario(AbstractRoutineInvocation<Long> query) {
            super(query);
        }

        @Override
        List<SqlParameter> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(Element element) throws SQLException {
            expect(element.getLong()).andReturn(123L);
        }
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

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
    }

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

}
