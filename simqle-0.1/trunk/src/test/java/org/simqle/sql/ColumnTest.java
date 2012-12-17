package org.simqle.sql;

import org.simqle.Element;
import org.simqle.Function;

import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 21.11.12
 * Time: 20:50
 * To change this template use File | Settings | File Templates.
 */
public class ColumnTest extends SqlTestCase {


    public void testValueFunctionality() throws Exception {
        final LongColumn col = createId();
        Element element = new ElementAdapter() {
            @Override
            public Long getLong() throws SQLException {
                return 1L;
            }
        };
        assertEquals(Long.valueOf(1), col.value(element));
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1", col.show());

    }

    public void testSelectStatementFunctionality() throws Exception {
        final LongColumn col = createId();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0", col.show());
    }

    private LongColumn createId() {
        return new LongColumn("id", person);
    }

    private LongColumn createAge() {
        return new LongColumn("age", person);
    }

    public void testSelectAll() throws Exception {
        final LongColumn col = createId();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0", col.all().show());

    }

    public void testSelectDistinct() throws Exception {
        final LongColumn col = createId();
        assertSimilar("SELECT DISTINCT T0.id AS C0 FROM person AS T0", col.distinct().show());
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = new FunctionCall<Long>("abs") {
            @Override
            public Long value(final Element element) throws SQLException {
                return element.getLong();
            }
        }.apply(createId()).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testAsFunctionMultipleArguments() throws Exception {
        final LongColumn column = createId();
        final String sql = new FunctionCall<Long>("max") {
            @Override
            public Long value(final Element element) throws SQLException {
                return element.getLong();
            }
        }.apply(column, column).show();
        assertSimilar("SELECT max(T0.id, T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final LongColumn id = createId();
        final String sql = id.where(id.booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testEq() throws Exception {
        final LongColumn id = createId();
        final LongColumn age = createAge();
        final String sql = id.where(id.eq(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id = T0.age", sql);
    }

    public void testNe() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(column.ne(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(column.gt(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id > T0.age", sql);
    }

    public void testGe() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(column.ge(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(column.lt(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id < T0.age", sql);
    }

    public void testLe() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(column.le(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <= T0.age", sql);
    }

    public void testExceptAll() throws Exception {
        final LongColumn column = createId();
        final String sql = column.exceptAll(new LongColumn("age", person2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final LongColumn column = createId();
        final String sql = column.exceptDistinct(new LongColumn("age", person2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final LongColumn column = createId();
        final String sql = column.except(new LongColumn("age", person2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final LongColumn column = createId();
        final String sql = column.unionAll(new LongColumn("age", person2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final LongColumn column = createId();
        final String sql = column.unionDistinct(new LongColumn("age", person2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final LongColumn column = createId();
        final String sql = column.union(new LongColumn("age", person2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final LongColumn column = createId();
        final String sql = column.intersectAll(new LongColumn("age", person2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersectDistinct() throws Exception {
        final LongColumn column = createId();
        final String sql = column.intersectDistinct(new LongColumn("age", person2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final LongColumn column = createId();
        final String sql = column.intersect(new LongColumn("age", person2)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testUseSameTableInDistinct() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.intersectDistinct(age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testSelectForUpdate() throws Exception {
        final LongColumn col = createId();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 FOR UPDATE", col.forUpdate().show());
    }

    public void testSelectForReadOnly() throws Exception {
        final LongColumn col = createId();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 FOR READ ONLY", col.forReadOnly().show());
    }

    public void testExists() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old
        final LongColumn age2 = new LongColumn("age", person2);
        String sql = id.where(age2.exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.age FROM person AS T1)", sql);

    }

    public void testExistsWithCondition() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        // find all but the most old
        final LongColumn age2 = new LongColumn("age", person2);
        String sql = id.where(age2.where(age2.gt(age)).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.age FROM person AS T1 WHERE T1.age > T0.age)", sql);

    }

    public void testInAll() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old
        final LongColumn id2 = new LongColumn("id", employee);
        String sql = id.where(id.in(id2.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old
        final LongColumn id2 = new LongColumn("id", employee);
        String sql = id.where(id.in(id2)).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id IN(SELECT T2.id FROM employee AS T2)", sql);
    }

    public void testNotInAll() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old
        final LongColumn id2 = new LongColumn("id", employee);
        String sql = id.where(id.notIn(id2.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id NOT IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old

        final ValueExpression<Long> expr = new LongParameter(1L);
        final ValueExpression<Long> expr2 = new LongParameter(2L);
        final ValueExpression<Long> expr3 = new LongParameter(3L);
        String sql = id.where(id.in(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old

        final ValueExpression<Long> expr = new LongParameter(1L);
        final ValueExpression<Long> expr2 = new LongParameter(2L);
        final ValueExpression<Long> expr3 = new LongParameter(3L);
        String sql = id.where(id.notIn(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.where(age.isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.age IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.where(age.isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.age IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.orderBy(age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age", sql);
    }

    public void testOrderByTwoColumns() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.orderBy(age, id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age, T0.id", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.orderBy(age.nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.orderBy(age.nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.orderBy(age.desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.orderBy(age.asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age ASC", sql);
    }

    public void testOperation() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.mult(age).show();
        assertSimilar("SELECT T0.id * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testFunction() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        FunctionCall<Long> sumOf = new FunctionCall<Long>("SUM_OF") {
            @Override
            public Long value(final Element element) throws SQLException {
                return element.getLong();
            }
        };
        String sql = sumOf.apply(id, age).show();
        assertSimilar("SELECT SUM_OF(T0.id, T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testPlus() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.plus(age).show();
        assertSimilar("SELECT T0.id + T0.age AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.minus(age).show();
        assertSimilar("SELECT T0.id - T0.age AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.mult(age).show();
        assertSimilar("SELECT T0.id * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.div(age).show();
        assertSimilar("SELECT T0.id / T0.age AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.concat(age).show();
        assertSimilar("SELECT T0.id || T0.age AS C0 FROM person AS T0", sql);
    }

    public void testJoin() throws Exception {
        final Person person1 = new Person();
        final Person person2 = new Person();
        final LongColumn id1 = new LongColumn("id", person1);
        final LongColumn id2 = new LongColumn("id", person2);
        final LongColumn parentId1 = new LongColumn("parent_id", person1);
        final LongColumn age1 = new LongColumn("age", person1);
        final LongColumn age2 = new LongColumn("age", person2);
        person1.leftJoin(person2, parentId1.eq(id2));
        // find all people who are older that parent
        final String sql = id1.where(age1.gt(age2)).show();
        System.out.println(sql);
        assertSimilar("SELECT T1.id AS C0 FROM person AS T1 LEFT JOIN person AS T2 ON T1.parent_id = T2.id WHERE T1.age > T2.age", sql);

    }

    public void testPair() throws Exception {
        final Person person = new Person();
        final LongColumn id = new LongColumn("id", person);
        final LongColumn age = new LongColumn("age", person);
        assertSimilar("SELECT T1.id AS C1, T1.age AS C2 FROM person AS T1", id.pair(age).show());
    }

    public void testConvert() throws Exception {
        final Person person = new Person();
        final LongColumn id = new LongColumn("id", person);
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1", id.convert(new Function<Long, String>() {
            @Override
            public String apply(final Long arg) {
                return String.valueOf(arg);
            }
        }).show());

    }


    private static class Person extends Table {
        private Person() {
            super("person");
        }
    }

    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
    }

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

}
