package org.simqle.integration;

import junit.framework.AssertionFailedError;
import org.simqle.Mappers;
import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.integration.model.MyDual;
import org.simqle.sql.AbstractValueExpression;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class ValueExpressionTest extends AbstractIntegrationTestBase {

    private AbstractValueExpression<Boolean> createVE(final Employee employee) {
        return employee.deptId.isNotNull().asValue();
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true, true, true, true), list);
    }

    public void testMap() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createVE(employee).map(Mappers.STRING).list(getDialectDataSource());
        Collections.sort(list);
        try {
            assertEquals(Arrays.asList("false", "true", "true", "true", "true"), list);
        } catch (AssertionFailedError e) {
            assertTrue(getDatabaseName(), getDatabaseName().equals("mysql"));
            assertEquals(Arrays.asList("0", "1", "1", "1", "1"), list);
        }
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).all().list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true, true, true, true), list);
    }

    public void testDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).distinct().list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true), list);
    }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).where(employee.lastName.eq("Cooper")).list(getDialectDataSource());
        assertEquals(Arrays.asList(false), list);
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).orderBy(employee.lastName.desc()).list(getDialectDataSource());
        assertEquals(Arrays.asList(true, true, true, true, false), list);
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Boolean,String>> list = createVE(employee).pair(employee.lastName).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make(false, "Cooper"), Pair.make(true, "First"), Pair.make(true, "March"), Pair.make(true, "Pedersen"), Pair.make(true, "Redwood")), list);

    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).isNull()).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(), list);

    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).isNotNull()).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).eq(employee.retired)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(), list);
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).ne(employee.retired)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).gt(employee.retired)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).ge(employee.retired)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).lt(employee.retired)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).le(employee.retired)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testEqValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).eq(false)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).ne(true)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).gt(true)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(), list);
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).ge(true)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).lt(true)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).le(true)).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee other = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).in(other.retired.where(other.lastName.eq("Cooper"))))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee other = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).notIn(other.retired.where(other.lastName.eq("Cooper"))))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final Employee other = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).in(true, false))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final Employee other = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).notIn(false))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }


    public void testOpposite() throws Exception {
        // what is the opposite to Boolean? many databases may not support it
        // but mysql allows it: - FALSE == FALSE; - TRUE == TRUE (conversion to 0/1, apply '-', convert back to Boolean -
        // everything not zero is true (like in C)
        try {
            final Employee employee = new Employee();
            final List<Pair<Boolean, String>> list = createVE(employee).opposite().pair(employee.lastName)
                    .where(employee.firstName.eq("James"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList(Pair.make(false, "Cooper"), Pair.make(true, "First")), list);
        } catch (SQLException e) {
            // ERROR 42X37: The unary '-' operator is not allowed on the 'BOOLEAN' type.
            expectSQLException(e, "derby");
        }
    }

    public void testAdd() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).add(employee.salary)
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(3001.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '+' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            expectSQLException(e, "derby");
        }

    }

    public void testSub() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).sub(employee.salary)
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(-2999.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '-' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            expectSQLException(e, "derby");
        }
    }

    public void testMult() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).mult(employee.salary)
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(0.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '*' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            expectSQLException(e, "derby");
        }
    }

    public void testDiv() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).div(employee.salary)
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(0.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '*' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            expectSQLException(e, "derby");
        }
    }

    public void testAddNumber() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).add(2.0)
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(3.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '+' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            expectSQLException(e, "derby");
        }

    }

    public void testSubNumber() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).sub(0.5)
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(0.5, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '-' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            expectSQLException(e, "derby");
        }
    }

    public void testMultNumber() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).mult(3.0)
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(0.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '*' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            expectSQLException(e, "derby");
        }
    }

    public void testDivNumber() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).div(3.0)
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(0.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '*' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            expectSQLException(e, "derby");
        }
    }

    public void testBooleanValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).booleanValue().negate()).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testConcat() throws Exception {
            final Employee employee = new Employee();
            final List<String> list = createVE(employee).concat(employee.lastName)
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
        try {
            assertEquals(Arrays.asList("falseCooper"), list);
        } catch (AssertionFailedError e) {
            if ("mysql".equals(getDatabaseName())) {
                assertEquals(Arrays.asList("0Cooper"), list);
            }
        }
    }

    public void testConcatString() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createVE(employee).concat("-")
                .where(employee.lastName.eq("Cooper"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        try {
            assertEquals(Arrays.asList("false-"), list);
        } catch (AssertionFailedError e) {
            if ("mysql".equals(getDatabaseName())) {
                assertEquals(Arrays.asList("0-"), list);
            }
        }
    }

    public void testOrderByArgument() throws Exception {
        final Employee employee = new Employee();
        System.out.println(employee.firstName.orderBy(createVE(employee), employee.firstName).show());
        final List<String> list = employee.firstName.orderBy(createVE(employee), employee.firstName).list(getDialectDataSource());
        assertEquals(Arrays.asList("James", "Alex", "Bill", "James", "Margaret"), list);

    }

    public void testOrderByNullsFirst() throws Exception {
        try {
            final Employee employee = new Employee();
            System.out.println(employee.firstName.orderBy(createVE(employee).nullsFirst(), employee.firstName).show());
            final List<String> list = employee.firstName.orderBy(createVE(employee), employee.firstName).list(getDialectDataSource());
            assertEquals(Arrays.asList("James", "Alex", "Bill", "James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByNullsLAst() throws Exception {
        try {
            final Employee employee = new Employee();
            System.out.println(employee.firstName.orderBy(createVE(employee).nullsLast(), employee.firstName).show());
            final List<String> list = employee.firstName.orderBy(createVE(employee), employee.firstName).list(getDialectDataSource());
            assertEquals(Arrays.asList("James", "Alex", "Bill", "James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testAsc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.orderBy(createVE(employee).asc(), employee.firstName).list(getDialectDataSource());
        assertEquals(Arrays.asList("James", "Alex", "Bill", "James", "Margaret"), list);
    }

    public void testDesc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.orderBy(createVE(employee).desc(), employee.firstName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret", "James"), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).unionAll(employee.retired.where(employee.lastName.eq("Cooper"))).list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true, true, true, true, true), list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).unionDistinct(employee.retired.where(employee.lastName.eq("Cooper"))).list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true), list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).union(employee.retired.where(employee.lastName.eq("Cooper"))).list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true), list);
    }

    public void testExceptAll() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Boolean> list = createVE(employee).exceptAll(employee.retired.where(employee.lastName.eq("Cooper"))).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(false, true, true, true), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExceptDistinct() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Boolean> list = createVE(employee).exceptDistinct(employee.retired.where(employee.lastName.eq("Cooper"))).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(false), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExcept() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Boolean> list = createVE(employee).except(employee.retired.where(employee.lastName.eq("Cooper"))).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(false), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectAll() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Boolean> list = createVE(employee).intersectAll(employee.retired.where(employee.lastName.eq("Cooper"))).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(true), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Boolean> list = createVE(employee).intersectDistinct(employee.retired.where(employee.lastName.eq("Cooper"))).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(true), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersect() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Boolean> list = createVE(employee).intersect(employee.retired.where(employee.lastName.eq("Cooper"))).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList(true), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testExists() throws Exception {
        final Employee employee = new Employee();
        final MyDual myDual = new MyDual();
        final List<String> list = myDual.dummy.where(createVE(employee).exists()).list(getDialectDataSource());
        assertEquals(Arrays.asList("X"), list);
    }

    public void testAsInArgument() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(department.manager().retired.in(createVE(employee)))
                .orderBy(department.deptName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testQueryValue() throws Exception {
        final MyDual myDual = new MyDual();
        final Department department = new Department();
        final AbstractValueExpression<Boolean> ve = myDual.dummy.eq("X").asValue();
        final List<Pair<Boolean, String>> list = ve.queryValue().pair(department.deptName)
                .orderBy(department.deptName).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make(true, "DEV"), Pair.make(true, "HR")), list);

    }

    public void testWhenClause() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Boolean, String>> list = employee.firstName.eq("James").then(createVE(employee)).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.make(false, "Cooper"),
                Pair.make(true, "First"),
                Pair.make((Boolean) null, "March"),
                Pair.make((Boolean) null, "Pedersen"),
                Pair.make((Boolean) null, "Redwood")), list);
    }

    public void testElse() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Boolean, String>> list = employee.firstName.ne("James").then(Params.p(false)).orElse(createVE(employee)).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.make(false, "Cooper"),
                Pair.make(true, "First"),
                Pair.make(false, "March"),
                Pair.make(false, "Pedersen"),
                Pair.make(false, "Redwood")), list);
    }

    public void testLike() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(createVE(employee).like("fa%"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            try {
                assertEquals(Arrays.asList("Cooper"), list);
            } catch (AssertionFailedError e) {
                if ("mysql".equals(getDatabaseName())) {
                    final List<String> mySqlList = employee.lastName
                            .where(createVE(employee).like("0%"))
                            .orderBy(employee.lastName)
                            .list(getDialectDataSource());
                    assertEquals(Arrays.asList("Cooper"), mySqlList);
                }
            }
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found
            expectSQLException(e, "derby");
        }
    }

    public void testNotLike() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(createVE(employee).notLike("tr%"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            try {
                assertEquals(Arrays.asList("Cooper"), list);
            } catch (AssertionFailedError e) {
                final List<String> mySqlList = employee.lastName
                        .where(createVE(employee).notLike("1%"))
                        .orderBy(employee.lastName)
                        .list(getDialectDataSource());
                assertEquals(Arrays.asList("Cooper"), mySqlList);
            }
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found
            expectSQLException(e, "derby");
        }
    }

    public void testCount() throws Exception {
        final Employee employee = new Employee();
        System.out.println(createVE(employee).count().show());
        final List<Integer> list = createVE(employee).count().list(getDialectDataSource());
        assertEquals(Arrays.asList(5), list);
    }

    public void testCountDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createVE(employee).countDistinct().list(getDialectDataSource());
        assertEquals(Arrays.asList(2), list);
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).min().list(getDialectDataSource());
        assertEquals(Arrays.asList(false), list);
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).max().list(getDialectDataSource());
        assertEquals(Arrays.asList(true), list);
    }

    public void testSum() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Number> list = createVE(employee).sum().list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(4.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y22: Aggregate SUM cannot operate on type BOOLEAN.
            expectSQLException(e, "derby");
        }
    }


    public void testAvg() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Number> list = createVE(employee).avg().list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(0.8, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y22: Aggregate AVG cannot operate on type BOOLEAN.
            expectSQLException(e, "derby");
        }
    }
}
