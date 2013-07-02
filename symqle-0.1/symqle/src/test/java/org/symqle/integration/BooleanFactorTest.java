package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.generic.Params;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractBooleanFactor;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class BooleanFactorTest extends AbstractIntegrationTestBase {

    /**
     * Returns condition, which is true for ["Cooper", "First", "Pedersen"]
     * @param employee
     * @return
     */
    private AbstractBooleanFactor createBasicCondition(final Employee employee) {
        final AbstractBooleanFactor negate = employee.hireDate.lt(new Date(108, 9, 1)).negate();
        return negate;
    }

    public void testAnd() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.and(employee.salary.gt(2500.0))).list(getDatabaseGate());
        assertEquals(Arrays.asList("First"), list);
    }

    public void testOr() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.or(employee.firstName.eq("Bill")))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
    }

    public void testNegate() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.negate()).orderBy(employee.lastName).list(getDatabaseGate());
        assertEquals(Arrays.asList("March", "Redwood"), list);

    }

    public void testIsTrue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isTrue())
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("Cooper", "First", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testIsNotTrue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotTrue())
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("March", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 96.
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testIsFalse() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isFalse())
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("March", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "FALSE" at line 1, column 92
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testIsNotFalse() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotFalse())
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("Cooper", "First", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testIsUnknown() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isUnknown())
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "UNKNOWN" at line 1, column 92.
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testIsNotUnknown() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotUnknown())
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(5, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "UNKNOWN" at line 1, column 96.
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.isNull())
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(0, list.size());
    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.isNotNull())
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(5, list.size());
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.eq(employee.salary.gt(2500.0)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March"), list);

    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.ne(employee.salary.gt(2500.0)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "Pedersen", "Redwood"), list);

    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.gt(employee.salary.gt(2500.0)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "Pedersen"), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.ge(employee.salary.gt(2500.0)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.lt(employee.salary.gt(2500.0)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.le(employee.salary.gt(2500.0)))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March", "Redwood"), list);
    }

    public void testEqValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.eq(false))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("March", "Redwood"), list);
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.ne(true))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("March", "Redwood"), list);
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.gt(false))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "Pedersen"), list);
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.ge(false))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.lt(true))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("March", "Redwood"), list);
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.le(false))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("March", "Redwood"), list);
    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee noDeptPeople = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        // the subquery result is {true}
        final List<String> list = employee.lastName
                .where(basicCondition.in(noDeptPeople.retired.where(noDeptPeople.deptId.isNull())))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "Pedersen"), list);
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee noDeptPeople = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        // the subquery result is {true}
        final List<String> list = employee.lastName
                .where(basicCondition.notIn(noDeptPeople.retired.where(noDeptPeople.deptId.isNull())))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("March", "Redwood"), list);
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final Employee noDeptPeople = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        // the subquery result is {true}
        final List<String> list = employee.lastName
                .where(basicCondition.in(true, (Boolean)null))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "Pedersen"), list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final Employee noDeptPeople = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        // the subquery result is {true}
        final List<String> list = employee.lastName
                .where(basicCondition.notIn(true))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("March", "Redwood"), list);
    }

    public void testThen() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<Pair<String,String>> list = employee.lastName.pair(basicCondition.then(employee.firstName))
                .orderBy(employee.lastName).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make("Cooper", "James"), Pair.make("First", "James"), Pair.make("March", null), Pair.make("Pedersen", "Alex"), Pair.make("Redwood", null)), list);
    }

    public void testThenNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBasicCondition(employee);
        final List<Pair<String,String>> list = employee.lastName.pair(
                    employee.salary.gt(2500.0).then(employee.firstName).orWhen(basicCondition.thenNull()).orElse(Params.p(":)"))
                )
                .orderBy(employee.lastName).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make("Cooper", null), Pair.make("First", "James"), Pair.make("March", ":)"), Pair.make("Pedersen", null), Pair.make("Redwood", "Margaret")), list);
    }
}
