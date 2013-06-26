package org.simqle.integration;

import org.simqle.Mappers;
import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.integration.model.MyDual;
import org.simqle.integration.model.One;
import org.simqle.mysql.MysqlDialect;
import org.simqle.sql.AbstractFactor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class FactorTest extends AbstractIntegrationTestBase {


    private AbstractFactor<Double> createFactor(final Employee employee) {
        return employee.salary.opposite();
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    public void testCast() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).cast("DECIMAL(7,2)").list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    public void testMap() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createFactor(employee).map(Mappers.INTEGER).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000, -3000, -2000, -2000, -1500), list);
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).all().list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    public void testDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).distinct().list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).where(employee.retired.booleanValue().negate()).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0), list);
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = createFactor(employee).pair(employee.lastName).where(employee.retired.booleanValue()).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(-1500.0, "Cooper")), list);
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractFactor<Integer> factor = employee.deptId.opposite();
        final List<String> list = employee.lastName.where(factor.isNull()).list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractFactor<Integer> factor = employee.deptId.opposite();
        final List<String> list = employee.lastName.where(factor.isNotNull()).orderBy(employee.lastName).list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createFactor(employee).eq(employee.salary)).list(getDatabaseGate());
        assertEquals(0, list.size());
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).ne(employee.salary))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).le(employee.salary))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).lt(employee.salary))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).ge(employee.salary))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(), list);
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).gt(employee.salary))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(), list);
    }

    public void testEqValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).eq(-1500.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).ne(-1500.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).ge(-2000.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).gt(-2000.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).le(-2000.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).lt(-2000.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee other = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).in(other.salary.opposite().where(other.retired.booleanValue().negate())))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee other = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).notIn(other.salary.opposite().where(other.retired.booleanValue().negate())))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).in(-2000.0, -1500.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).notIn(-2000.0, -1500.0))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).opposite().list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testAdd() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).add(employee.salary.mult(2)).list(getDatabaseGate());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, actual);
    }

    public void testSub() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).sub(employee.salary.mult(2).opposite()).list(getDatabaseGate());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, actual);
    }

    public void testMult() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).mult(employee.salary.div(createFactor(employee))).list(getDatabaseGate());
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
        final List<Number> list = createFactor(employee).div(createFactor(employee).div(employee.salary)).list(getDatabaseGate());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, actual);
    }

    public void testAddNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).add(3000.0).list(getDatabaseGate());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(0.0, 0.0, 1000.0, 1000.0, 1500.0);
        assertEquals(expected, actual);
    }

    public void testSubNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).sub(500.0).list(getDatabaseGate());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(-3500.0, -3500.0, -2500.0, -2500.0, -2000.0);
        assertEquals(expected, actual);
    }

    public void testMultNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).mult(2).list(getDatabaseGate());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(-6000.0, -6000.0, -4000.0, -4000.0, -3000.0);
        assertEquals(expected, actual);
    }

    public void testDivNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).div(0.5).list(getDatabaseGate());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(-6000.0, -6000.0, -4000.0, -4000.0, -3000.0);
        assertEquals(expected, actual);
    }

    public void testBooleanValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createFactor(employee).booleanValue())
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
            final List<String> list = createFactor(employee).concat(employee.lastName)
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("-1500Cooper", "-3000First", "-2000March", "-2000Pedersen", "-3000Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'
            expectSQLException(e, "derby");
        }
    }

    public void testConcatString() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createFactor(employee).concat(" marsian $")
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("-1500 marsian $", "-3000 marsian $", "-2000 marsian $", "-2000 marsian $", "-3000 marsian $"), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'
            expectSQLException(e, "derby");
        }
    }

    public void testCollate() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createFactor(employee).map(Mappers.STRING).collate("latin1_general_ci")
                    .concat(" marsian $")
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("-1500 marsian $", "-3000 marsian $", "-2000 marsian $", "-2000 marsian $", "-3000 marsian $"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE" at line 1, column 21.
            expectSQLException(e, "derby");
        }
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).orderBy(createFactor(employee)).list(getDatabaseGate());
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    public void testOrderAsc() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).orderAsc().list(getDatabaseGate());
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    public void testOrderDesc() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).orderDesc().list(getDatabaseGate());
        assertEquals(Arrays.asList(-1500.0, -2000.0, -2000.0, -3000.0, -3000.0), list);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = createFactor(employee).orderBy(createFactor(employee).nullsFirst()).list(getDatabaseGate());
            assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = createFactor(employee).orderBy(createFactor(employee).nullsLast()).list(getDatabaseGate());
            assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByAsc() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).orderBy(createFactor(employee).asc()).list(getDatabaseGate());
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    public void testOrderByDesc() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).orderBy(createFactor(employee).desc()).list(getDatabaseGate());
        assertEquals(Arrays.asList(-1500.0, -2000.0, -2000.0, -3000.0, -3000.0), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).unionAll(employee.salary.where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0, 1500.0), list);

    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).unionDistinct(employee.salary.opposite().where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);

    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).union(employee.salary.opposite().where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);

    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = createFactor(employee).exceptAll(createFactor(employee).where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }

    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = createFactor(employee).exceptDistinct(createFactor(employee).where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }

    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = createFactor(employee).except(createFactor(employee).where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = createFactor(employee).intersectAll(createFactor(employee).where(employee.lastName.ne("Cooper"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = createFactor(employee).intersectDistinct(createFactor(employee).where(employee.lastName.ne("Cooper"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = createFactor(employee).intersect(createFactor(employee).where(employee.lastName.ne("Cooper"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testExists() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createFactor(employee).exists())
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testContains() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createFactor(employee).contains(-3000.0))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).forUpdate().list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = createFactor(employee).forReadOnly().list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
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
            createFactor(employee).queryValue().pair(department.deptName).list(getDatabaseGate());
            fail("Scalar subquery is only allowed to return a single row");
        } catch (SQLException e) {
            // fine
        }
    }

    public void testQueryValue() throws Exception {
        final List<Pair<Integer, String>> list = new One().id.opposite().queryValue().pair(new MyDual().dummy)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(-1, "X")), list);
    }

    public void testWhenClause() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.retired.booleanValue().then(createFactor(employee)).orElse(employee.salary).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testElse() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.retired.booleanValue().negate().then(employee.salary).orElse(createFactor(employee)).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(-1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testLike() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createFactor(employee).like(Params.p("-2%")))
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
            final List<String> list = employee.lastName.where(createFactor(employee).like("-2%"))
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
            final List<String> list = employee.lastName.where(createFactor(employee).notLike(Params.p("-2%")))
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
            final List<String> list = employee.lastName.where(createFactor(employee).notLike("-2%"))
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
        final List<Integer> list = createFactor(employee).count().list(getDatabaseGate());
        assertEquals(Arrays.asList(5), list);
    }

    public void testCountDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createFactor(employee).countDistinct().list(getDatabaseGate());
        assertEquals(Arrays.asList(3), list);
    }

    public void testAverage() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).avg().list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(-2300.0, list.get(0).doubleValue());
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).min().list(getDatabaseGate());
        assertEquals(Arrays.asList(-3000.0), list);
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).max().list(getDatabaseGate());
        assertEquals(Arrays.asList(-1500.0), list);
    }
}
