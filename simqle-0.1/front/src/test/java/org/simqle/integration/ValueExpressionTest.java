package org.simqle.integration;

import junit.framework.AssertionFailedError;
import org.simqle.Pair;
import org.simqle.integration.model.Employee;
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
        assertEquals(Arrays.asList(Pair.of(false, "Cooper"), Pair.of(true, "First"), Pair.of(true, "March"), Pair.of(true, "Pedersen"), Pair.of(true, "Redwood")), list);

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
            assertEquals(Arrays.asList(Pair.of(false, "Cooper"), Pair.of(true, "First")), list);
        } catch (SQLException e) {
            // ERROR 42X37: The unary '-' operator is not allowed on the 'BOOLEAN' type.
            expectSQLException(e, "derby");
        }
    }

    public void testPlus() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).plus(employee.salary)
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

    public void testMinus() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).minus(employee.salary)
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

    public void testPlusNumber() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).plus(2.0)
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

    public void testMinusNumber() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).minus(0.5)
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
}
