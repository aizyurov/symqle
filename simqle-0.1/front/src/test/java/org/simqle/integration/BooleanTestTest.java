package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.integration.model.Employee;
import org.simqle.sql.AbstractBooleanTest;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class BooleanTestTest extends AbstractIntegrationTestBase {

    /**
     * Returns condition, which is true for ["Cooper"]
     * @param employee
     * @return
     */
    private AbstractBooleanTest createBasicCondition(final Employee employee) {
        return employee.retired.booleanValue().isTrue();
    }

    public void testAnd() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.and(employee.salary.gt(2500.0))).list(getDialectDataSource());
            assertEquals(Arrays.asList(), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testOr() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.or(employee.firstName.eq("Bill")))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "March"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testNegate() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.negate()).orderBy(employee.lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }

    }

    public void testIsTrue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isTrue())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, "derby");
        }
    }

    public void testIsNotTrue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotTrue())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 96.
            expectSQLException(e, "derby");
        }
    }

    public void testIsFalse() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isFalse())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "FALSE" at line 1, column 92
            expectSQLException(e, "derby");
        }
    }

    public void testIsNotFalse() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotFalse())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, "derby");
        }
    }

    public void testIsUnknown() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isUnknown())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testIsNotUnknown() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotUnknown())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(5, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNull())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotNull())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(5, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.eq(employee.salary.le(2500.0)))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }

    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.ne(employee.salary.gt(2500.0)))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }

    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.gt(employee.salary.gt(2500.0)))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.ge(employee.salary.gt(2500.0)))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.lt(employee.salary.gt(2500.0)))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.le(employee.salary.gt(2500.0)))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testEqValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.eq(false))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.ne(true))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.gt(false))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.ge(false))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.lt(true))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.le(false))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee noDeptPeople = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        // the subquery result is {true}
        try {
            final List<String> list = employee.lastName
                    .where(basicCondition.in(noDeptPeople.retired.where(noDeptPeople.deptId.isNull())))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee noDeptPeople = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        // the subquery result is {true}
        try {
            final List<String> list = employee.lastName
                    .where(basicCondition.notIn(noDeptPeople.retired.where(noDeptPeople.deptId.isNull())))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final Employee noDeptPeople = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        // the subquery result is {true}
        try {
            final List<String> list = employee.lastName
                    .where(basicCondition.in(true, (Boolean) null))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final Employee noDeptPeople = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        // the subquery result is {true}
        try {
            final List<String> list = employee.lastName
                    .where(basicCondition.notIn(true))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testThen() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<Pair<String,String>> list = employee.lastName.pair(basicCondition.then(employee.firstName))
                    .orderBy(employee.lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(Pair.of("Cooper", "James"), Pair.of("First", null), Pair.of("March", null), Pair.of("Pedersen", null), Pair.of("Redwood", null)), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }

    public void testThenNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanTest basicCondition = createBasicCondition(employee);
        try {
            final List<Pair<String,String>> list = employee.lastName.pair(
                        employee.salary.gt(2500.0).then(employee.firstName).orWhen(basicCondition.thenNull()).orElse(Params.p(":)"))
                    )
                    .orderBy(employee.lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(Pair.of("Cooper", null), Pair.of("First", "James"), Pair.of("March", ":)"), Pair.of("Pedersen", ":)"), Pair.of("Redwood", "Margaret")), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column ...
            expectSQLException(e, "derby");
        }
    }
}
