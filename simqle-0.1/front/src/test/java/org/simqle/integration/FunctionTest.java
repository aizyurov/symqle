package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.integration.model.One;
import org.simqle.mysql.MysqlDialect;
import org.simqle.sql.AbstractRoutineInvocation;
import org.simqle.sql.SqlFunction;
import org.simqle.sql.ValueExpression;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class FunctionTest extends AbstractIntegrationTestBase {

    private <T> AbstractRoutineInvocation<T> abs(ValueExpression<T> e) {
        return SqlFunction.create("abs", e.getMapper()).apply(e);
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

   public void testAll() throws Exception {
       final Employee employee = new Employee();
       final List<Double> list = abs(employee.salary.opposite()).all().list(getDialectDataSource());
       Collections.sort(list);
       assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

   public void testDistinct() throws Exception {
       final Employee employee = new Employee();
       final List<Double> list = abs(employee.salary.opposite()).distinct().list(getDialectDataSource());
       Collections.sort(list);
       assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
   }

    public void testAsFunctionArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(abs(employee.salary.opposite())).list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testBooleanValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).booleanValue())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: Cannot convert types 'DOUBLE' to 'BOOLEAN'.
            expectSQLException(e, "derby");
        }
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).eq(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).ne(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(), list);
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).gt(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).ge(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).lt(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).le(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testEqValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).eq(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).ne(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).gt(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).ge(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).lt(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).le(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee hr = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).in(hr.salary.where(hr.department().deptName.eq("HR"))))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).in(1500.0, 2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee hr = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).notIn(hr.salary.where(hr.department().deptName.eq("HR"))))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).notIn(1500.0, 2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.deptId).isNull())
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
   }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.deptId).isNotNull())
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
   }

    public void testAsSortSpec() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(abs(employee.salary.opposite()), employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(abs(employee.deptId).nullsFirst(), employee.lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "March", "Redwood", "First", "Pedersen"), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(abs(employee.deptId).nullsLast(), employee.lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Redwood", "First", "Pedersen", "Cooper"), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testAsc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(abs(employee.salary.opposite()).asc(), employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
    }

    public void testDesc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(abs(employee.salary.opposite()).desc(), employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "Redwood", "March", "Pedersen", "Cooper"), list);
    }

    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).opposite().list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double,String>> list = abs(employee.salary.opposite()).pair(employee.lastName)
                .where(employee.deptId.isNull())
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make(1500.0, "Cooper")), list);
    }

    public void testAdd() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).add(employee.department().manager().salary)
                .orderBy(employee.lastName).list(getDialectDataSource());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(null, 6000.0, 5000.0, 5000.0, 6000.0), asDoubles);
    }

    public void testAddNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).add(400.0)
                .orderBy(employee.lastName).list(getDialectDataSource());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(1900.0, 3400.0, 2400.0, 2400.0, 3400.0), asDoubles);
    }

    public void testSub() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).sub(employee.department().manager().salary)
                .orderBy(employee.lastName).list(getDialectDataSource());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(null, 0.0, -1000.0, -1000.0, 0.0), asDoubles);
    }

    public void testSubNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).sub(100.0)
                .orderBy(employee.lastName).list(getDialectDataSource());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(1400.0, 2900.0, 1900.0, 1900.0, 2900.0), asDoubles);
    }

    public void testMult() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).mult(employee.department().manager().salary)
                .orderBy(employee.lastName).list(getDialectDataSource());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(null, 9000000.0, 6000000.0, 6000000.0, 9000000.0), asDoubles);
    }

    public void testMultNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).mult(2.0).orderBy(employee.lastName).list(getDialectDataSource());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(3000.0, 6000.0, 4000.0, 4000.0, 6000.0), asDoubles);
    }

    public void testDiv() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).div(employee.department().manager().salary.div(100)).orderBy(employee.lastName).list(getDialectDataSource());
        final List<Integer> percentList = new ArrayList<Integer>();
        for (Number n : list) {
            percentList.add(n == null ? null : n.intValue());
        }
        assertEquals(Arrays.asList(null, 100, 66, 66, 100), percentList);
    }

    public void testDivNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).div(0.5)
                .orderBy(employee.lastName).list(getDialectDataSource());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(3000.0, 6000.0, 4000.0, 4000.0, 6000.0), asDoubles);
    }

    public void testConcatString() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = abs(employee.salary.opposite()).concat(" marsian dollars")
                    .orderBy(employee.lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList("1500 marsian dollars", "3000 marsian dollars", "2000 marsian dollars", "2000 marsian dollars", "3000 marsian dollars"), list);
        } catch (SQLException e) {
            // derby: Cannot convert types 'DOUBLE' to 'VARCHAR'.
            expectSQLException(e, "derby");
        }

    }

    public void testConcat() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = abs(employee.salary.opposite()).concat(employee.lastName)
                    .orderBy(employee.lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList("1500Cooper", "3000First", "2000March", "2000Pedersen", "3000Redwood"), list);
        } catch (SQLException e) {
            // derby: Cannot convert types 'DOUBLE' to 'VARCHAR'.
            expectSQLException(e, "derby");
        }
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).unionAll(employee.salary.where(employee.lastName.eq("Cooper")))
                .list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).unionDistinct(employee.salary.where(employee.lastName.eq("Cooper")))
                .list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).union(employee.salary.where(employee.lastName.eq("Cooper")))
                .list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).exceptAll(employee.salary.where(employee.lastName.eq("Cooper")))
                    .list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0, 2000.0, 3000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).exceptDistinct(employee.salary.where(employee.lastName.eq("Cooper")))
                    .list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).except(employee.salary.where(employee.lastName.eq("Cooper")))
                    .list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).intersectAll(employee.salary.where(employee.lastName.eq("March").or(employee.lastName.eq("Pedersen"))))
                    .list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0, 2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).intersectDistinct(employee.salary.where(employee.lastName.eq("March").or(employee.lastName.eq("Pedersen"))))
                    .list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).intersect(employee.salary.where(employee.lastName.eq("March").or(employee.lastName.eq("Pedersen"))))
                    .list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).forUpdate().list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).forReadOnly().list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
        } catch (SQLException e) {
            if (MysqlDialect.class.equals(getDialectDataSource().getDialect().getClass())) {
                // should work with MysqlDialect
                throw e;
            } else {
                // mysql does not support FOR READ ONLY natively
                expectSQLException(e, "mysql");
            }
        }
    }

    public void testExists() throws Exception {
        final Employee employee = new Employee();
        final One one = new One();
        final List<String> list = employee.lastName.where(abs(one.id).exists()).list(getDialectDataSource());
        assertEquals(5, list.size());
    }

    public void testQueryValue() throws Exception {
        final Department department = new Department();
        final One one = new One();
        final List<Pair<String, Integer>> list = department.deptName.pair(abs(one.id.opposite()).queryValue())
                .orderBy(department.deptName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make("DEV", 1), Pair.make("HR", 1)), list);
    }

    public void testLike() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(abs(employee.salary).like(Params.p("20%")))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby : No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            expectSQLException(e, "derby");
        }
    }

    public void testNotLike() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(abs(employee.salary).notLike(Params.p("20%")))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby : No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            expectSQLException(e, "derby");
        }
    }

    public void testLikeString() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(abs(employee.salary).like("20%"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby : No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            expectSQLException(e, "derby");
        }
    }

    public void testNotLikeString() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(abs(employee.salary).notLike("20%"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby : No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            expectSQLException(e, "derby");
        }
    }

    public void testCount() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = abs(employee.salary).count().list(getDialectDataSource());
        assertEquals(Arrays.asList(5), list);
    }

    public void testSum() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary).sum().list(getDialectDataSource());
        assertEquals(1, list.size());
        assertEquals(11500, list.get(0).intValue());
    }

    public void testAvg() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary).avg().list(getDialectDataSource());
        assertEquals(1, list.size());
        assertEquals(2300, list.get(0).intValue());
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary).min().list(getDialectDataSource());
        assertEquals(Arrays.asList(1500.0), list);
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary).max().list(getDialectDataSource());
        assertEquals(Arrays.asList(3000.0), list);
    }
}
