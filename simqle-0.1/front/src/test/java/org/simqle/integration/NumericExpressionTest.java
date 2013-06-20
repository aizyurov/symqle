package org.simqle.integration;

import org.simqle.Mappers;
import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.integration.model.MyDual;
import org.simqle.integration.model.One;
import org.simqle.mysql.MysqlDialect;
import org.simqle.sql.AbstractNumericExpression;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class NumericExpressionTest extends AbstractIntegrationTestBase {


    private AbstractNumericExpression<Number> createExpression(final Employee employee) {
        return employee.salary.add(100);
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    public void testMap() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createExpression(employee).map(Mappers.DOUBLE).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).all().list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    public void testDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).distinct().list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(1600.0, 2100.0, 3100.0), list);
    }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee)
                .where(employee.retired.booleanValue().negate()).list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Number, String>> list = createExpression(employee).pair(employee.lastName).where(employee.retired.booleanValue()).list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals("Cooper", list.get(0).second());
        assertEquals(1600, list.get(0).first().intValue());
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractNumericExpression<Number> expression = employee.deptId.add(1);
        final List<String> list = employee.lastName.where(expression.isNull()).list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractNumericExpression<Number> expression = employee.deptId.add(1);
        final List<String> list = employee.lastName.where(expression.isNotNull()).orderBy(employee.lastName).list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createExpression(employee).eq(employee.salary.sub(50))).list(getDatabaseGate());
        assertEquals(0, list.size());
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).ne(employee.salary.sub(50)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).le(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).lt(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).ge(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(), list);
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).gt(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(), list);
    }

    public void testEqValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).eq(1600.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).ne(1600.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).ge(2100.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).gt(2100.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).le(2100.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).lt(2100.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee other = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).in(other.salary.add(100).where(other.retired.booleanValue().negate())))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee other = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).notIn(other.salary.add(100).where(other.retired.booleanValue().negate())))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).in(1600.0, 2100.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).notIn(1600.0, 2100.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).opposite().list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3100.0, -3100.0, -2100.0, -2100.0, -1600.0), list);
    }

    public void testAdd() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).add(employee.salary.mult(2)).list(getDatabaseGate()));
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(4600.0, 6100.0, 6100.0, 9100.0, 9100.0);
        assertEquals(expected, list);
    }

    public void testSub() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).sub(employee.salary).list(getDatabaseGate()));
        final List<Double> expected = Arrays.asList(100.0, 100.0, 100.0, 100.0, 100.0);
        assertEquals(expected, list);
    }

    public void testMult() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee).mult(employee.salary.div(createExpression(employee))).list(getDatabaseGate());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, actual);
    }

    public void testDiv() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = toListOfDouble(createExpression(employee).div(department.deptId.count().queryValue()).list(getDatabaseGate()));
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(800.0, 1050.0, 1050.0, 1550.0, 1550.0);
        assertEquals(expected, list);
    }

    public void testAddNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).add(100.0).list(getDatabaseGate()));
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(1700.0, 2200.0, 2200.0, 3200.0, 3200.0);
        assertEquals(expected, list);
    }

    public void testSubNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).sub(500.0).list(getDatabaseGate()));
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(1100.0, 1600.0, 1600.0, 2600.0, 2600.0);
        assertEquals(expected, list);
    }

    public void testMultNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).mult(2).list(getDatabaseGate()));
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(3200.0, 4200.0, 4200.0, 6200.0, 6200.0);
        assertEquals(expected, list);
    }

    public void testDivNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).div(0.5).list(getDatabaseGate()));
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(3200.0, 4200.0, 4200.0, 6200.0, 6200.0);
        assertEquals(expected, list);
    }

    public void testBooleanValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createExpression(employee).booleanValue())
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby:GenericDialect ERROR 42X19: The WHERE or HAVING clause or CHECK CONSTRAINT definition is a 'DOUBLE' expression.  It must be a BOOLEAN expression.
            // derby:DerbyDialect ERROR 42846: Cannot convert types 'DOUBLE' to 'BOOLEAN'.
            expectSQLException(e, "derby");
        }
    }

    public void testConcat() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createExpression(employee).concat(employee.lastName)
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("1600Cooper", "3100First", "2100March", "2100Pedersen", "3100Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'
            expectSQLException(e, "derby");
        }
    }

    public void testConcatString() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createExpression(employee).concat(" marsian $")
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("1600 marsian $", "3100 marsian $", "2100 marsian $", "2100 marsian $", "3100 marsian $"), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'
            expectSQLException(e, "derby");
        }
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).orderBy(createExpression(employee)).list(getDatabaseGate()));
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = toListOfDouble(createExpression(employee).orderBy(createExpression(employee).nullsFirst()).list(getDatabaseGate()));
            assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = toListOfDouble(createExpression(employee).orderBy(createExpression(employee).nullsLast()).list(getDatabaseGate()));
            assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByAsc() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).orderBy(createExpression(employee).asc()).list(getDatabaseGate()));
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    public void testOrderByDesc() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).orderBy(createExpression(employee).desc()).list(getDatabaseGate()));
        assertEquals(Arrays.asList(3100.0, 3100.0, 2100.0, 2100.0, 1600.0), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).unionAll(employee.salary.add(100).where(employee.lastName.eq("Cooper"))).list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(1600.0, 1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);

    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).unionDistinct(employee.salary.add(100).where(employee.lastName.eq("Cooper"))).list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(1600.0, 2100.0, 3100.0), list);

    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).union(employee.salary.add(100).where(employee.lastName.eq("Cooper"))).list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(1600.0, 2100.0, 3100.0), list);
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = toListOfDouble(createExpression(employee).exceptAll(createExpression(employee).where(employee.lastName.eq("Cooper"))).list(getDatabaseGate()));
            Collections.sort(list);
            assertEquals(Arrays.asList(2100.0, 2100.0, 3100.0, 3100.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }

    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = toListOfDouble(createExpression(employee).exceptDistinct(createExpression(employee).where(employee.lastName.eq("Cooper"))).list(getDatabaseGate()));
            Collections.sort(list);
            assertEquals(Arrays.asList(2100.0, 3100.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = toListOfDouble(createExpression(employee).except(createExpression(employee).where(employee.lastName.eq("Cooper"))).list(getDatabaseGate()));
            Collections.sort(list);
            assertEquals(Arrays.asList(2100.0, 3100.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = toListOfDouble(createExpression(employee).intersectAll(createExpression(employee).where(employee.lastName.ne("Cooper"))).list(getDatabaseGate()));
            Collections.sort(list);
            assertEquals(Arrays.asList(2100.0, 2100.0, 3100.0, 3100.0), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = toListOfDouble(createExpression(employee).intersectDistinct(createExpression(employee).where(employee.lastName.ne("Cooper"))).list(getDatabaseGate()));
            Collections.sort(list);
            assertEquals(Arrays.asList(2100.0, 3100.0), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = toListOfDouble(createExpression(employee).intersect(createExpression(employee).where(employee.lastName.ne("Cooper"))).list(getDatabaseGate()));
            Collections.sort(list);
            assertEquals(Arrays.asList(2100.0, 3100.0), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testExists() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createExpression(employee).exists())
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).forUpdate().list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = toListOfDouble(createExpression(employee).forReadOnly().list(getDatabaseGate()));
            Collections.sort(list);
            assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
        } catch (SQLException e) {
            if (MysqlDialect.class.equals(getDatabaseGate().getDialect().getClass())) {
                // should work with MysqlDialect
                throw e;
            } else {
                // mysql does not support FOR READ ONLY natively
                expectSQLException(e, "mysql");
            }
        }
    }

    public void testQueryValueNegative() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            createExpression(employee).queryValue().pair(department.deptName).list(getDatabaseGate());
            fail("Scalar subquery is only allowed to return a single row");
        } catch (SQLException e) {
            // fine
        }
    }

    public void testQueryValue() throws Exception {
        final List<Pair<Number, String>> list = new One().id.add(10).queryValue().pair(new MyDual().dummy)
                .list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(11, list.get(0).first().intValue());
        assertEquals("X", list.get(0).second());
    }

    public void testWhenClause() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(employee.retired.booleanValue().negate().then(createExpression(employee)).orElse(employee.salary.sub(100)).list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(1400.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    public void testElse() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(employee.retired.booleanValue().negate().then(employee.salary.add(200)).orElse(createExpression(employee)).list(getDatabaseGate()));
        Collections.sort(list);
        assertEquals(Arrays.asList(1600.0, 2200.0, 2200.0, 3200.0, 3200.0), list);
    }

    public void testLike() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createExpression(employee).like(Params.p("21%")))
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            expectSQLException(e, "derby");
        }
    }

    public void testLikeString() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createExpression(employee).like("21%"))
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            expectSQLException(e, "derby");
        }
    }

    public void testNotLike() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createExpression(employee).notLike(Params.p("21%")))
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            expectSQLException(e, "derby");
        }
    }

    public void testNotLikeString() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createExpression(employee).notLike("21%"))
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            expectSQLException(e, "derby");
        }
    }

    public void testCount() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createExpression(employee).count().list(getDatabaseGate());
        assertEquals(Arrays.asList(5), list);
    }

    public void testCountDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createExpression(employee).countDistinct().list(getDatabaseGate());
        assertEquals(Arrays.asList(3), list);
    }

    public void testAverage() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee).avg().list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(2400.0, list.get(0).doubleValue());
    }

    public void testSum() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee).sum().list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(12000.0, list.get(0).doubleValue());
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).min().list(getDatabaseGate()));
        assertEquals(Arrays.asList(1600.0), list);
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).max().list(getDatabaseGate()));
        assertEquals(Arrays.asList(3100.0), list);
    }
}
