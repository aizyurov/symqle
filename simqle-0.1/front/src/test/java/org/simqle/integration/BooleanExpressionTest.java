package org.simqle.integration;

import org.simqle.integration.model.Employee;
import org.simqle.sql.AbstractBooleanExpression;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class BooleanExpressionTest extends AbstractIntegrationTestBase {

    public void testAnd() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanExpression basicCondition = employee.retired.booleanValue().or(employee.hireDate.ge(new Date(108, 9, 1)));
        final List<String> list = employee.lastName.where(basicCondition.and(employee.salary.gt(2500.0))).list(getDialectDataSource());
        assertEquals(Arrays.asList("First"), list);
    }

    public void testOr() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanExpression basicCondition = employee.retired.booleanValue().or(employee.hireDate.ge(new Date(108, 9, 1)));
        final List<String> list = employee.lastName.where(basicCondition.or(employee.firstName.eq("Bill")))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
    }

    public void testNegate() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanExpression basicCondition = employee.retired.booleanValue().or(employee.hireDate.ge(new Date(108, 9, 1)));
        final List<String> list = employee.lastName.where(basicCondition.negate()).orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("March", "Redwood"), list);

    }

    public void testIsTrue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanExpression basicCondition = employee.retired.booleanValue().or(employee.hireDate.ge(new Date(108, 9, 1)));
        try {
            final List<String> list = employee.lastName.where(basicCondition.isTrue())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, "derby");
        }
    }

    public void testIsNotTrue() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanExpression basicCondition = employee.retired.booleanValue().or(employee.hireDate.ge(new Date(108, 9, 1)));
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotTrue())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 96.
            expectSQLException(e, "derby");
        }
    }

    public void testIsFalse() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanExpression basicCondition = employee.retired.booleanValue().or(employee.hireDate.ge(new Date(108, 9, 1)));
        try {
            final List<String> list = employee.lastName.where(basicCondition.isFalse())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "FALSE" at line 1, column 92
            expectSQLException(e, "derby");
        }
    }

    public void testIsNotFalse() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanExpression basicCondition = employee.retired.booleanValue().or(employee.hireDate.ge(new Date(108, 9, 1)));
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotFalse())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, "derby");
        }
    }

    public void testIsUnknown() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanExpression basicCondition = employee.retired.booleanValue().or(employee.hireDate.ge(new Date(108, 9, 1)));
        try {
            final List<String> list = employee.lastName.where(basicCondition.isUnknown())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "UNKNOWN" at line 1, column 92.
            expectSQLException(e, "derby");
        }
    }

    public void testIsNotUnknown() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanExpression basicCondition = employee.retired.booleanValue().or(employee.hireDate.ge(new Date(108, 9, 1)));
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotUnknown())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(5, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "UNKNOWN" at line 1, column 96.
            expectSQLException(e, "derby");
        }
    }


}
