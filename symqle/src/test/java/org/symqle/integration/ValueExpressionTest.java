package org.symqle.integration;

import junit.framework.AssertionFailedError;
import org.symqle.common.Callback;
import org.symqle.common.CoreMappers;
import org.symqle.common.Pair;
import org.symqle.sql.Params;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.sql.AbstractValueExpression;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Label;
import org.symqle.sql.SelectStatement;
import org.symqle.testset.AbstractValueExpressionTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class ValueExpressionTest extends AbstractIntegrationTestBase implements AbstractValueExpressionTestSet {

    private AbstractValueExpression<Boolean> createVE(final Employee employee) {
        return employee.deptId.isNotNull().asValue();
    }

    @Override
    public void test_add_Number() throws Exception {
                // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).add(2.0)
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(3.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '+' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean + numeric
            // org.h2.jdbc.JdbcSQLException: Feature not supported: "BOOLEAN +"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = employee.salary.add(createVE(employee))
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(3001.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '+' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean + double precision
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_add_Term() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).add(employee.salary)
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(3001.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '+' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean + double precision
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James").asValue().in(
                employee.department().deptName.eq("HR").asValue().asInValueList()
                        .append(createVE(employee))))
                .orderBy(employee.lastName)
                .list(getEngine());
        // Cooper: true in { false, false }
        // First: true in { false, true }
        // March: false in { true, true }
        // Pedersen: false in { false, true }
        // Redwood: false in { true, true }
        assertEquals(Arrays.asList("First", "Pedersen"), list);
    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James").asValue().in(
                employee.department().deptName.eq("HR").asValue().asInValueList()
                        .append(createVE(employee))))
                .orderBy(employee.lastName)
                .list(getEngine());
        // Cooper: true in { false, false }
        // First: true in { false, true }
        // March: false in { true, true }
        // Pedersen: false in { false, true }
        // Redwood: false in { true, true }
        assertEquals(Arrays.asList("First", "Pedersen"), list);
    }

    @Override
    public void test_asBoolean_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).asBoolean())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);


    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.orderBy(createVE(employee).asc(), employee.firstName).list(getEngine());
        assertEquals(Arrays.asList("James", "Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Number> list = createVE(employee).avg().list(getEngine());
            assertEquals(1, list.size());
            assertEquals(0.8, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y22: Aggregate AVG cannot operate on type BOOLEAN.
            // org.postgresql.util.PSQLException: ERROR: function avg(boolean) does not exist
            // org.h2.jdbc.JdbcSQLException: Feature not supported: "BOOLEAN +"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createVE(employee).cast("CHAR(5)").map(CoreMappers.STRING).list(getEngine());
        Collections.sort(list);
        final List<String> expected;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            expected = Arrays.asList("0", "1", "1", "1", "1");
        } else if (SupportedDb.H2.equals(getDatabaseName())) {
            expected = Arrays.asList("FALSE", "TRUE", "TRUE", "TRUE", "TRUE");
        } else {
            expected = Arrays.asList("false", "true ", "true ", "true ", "true ");
        }
        assertEquals(expected, list);
    }

    @Override
    public void test_charLength_() throws Exception {
        try {
            final Employee employee = new Employee();
            final int expectedLength;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                // "0"
                expectedLength = 1;
            } else if (SupportedDb.APACHE_DERBY.equals(getDatabaseName())) {
                // do not know why
                expectedLength = 1;
            } else {
                // "TRUE" or "true"
                expectedLength = 4;
            }
            final List<Integer> list = createVE(employee).charLength().where(employee.lastName.eq("Redwood"))
                    .list(getEngine());
            assertEquals(Arrays.asList(expectedLength), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function char_length(boolean) does not exist
            expectSQLException(e, SupportedDb.POSTGRESQL);
       }
    }

    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createVE(employee)
                    .collate(validCollationNameForNumber())
                    .concat("-")
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            try {
                assertEquals(Arrays.asList("false-"), list);
            } catch (AssertionFailedError e) {
                if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                    assertEquals(Arrays.asList("0-"), list);
                }
            }
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE" at line 1, column 34.
            // org.postgresql.util.PSQLException: ERROR: collations are not supported by type boolean
            // org.h2.jdbc.JdbcSQLException: Syntax error in SQL statement
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).compileQuery(getEngine())
                .list();
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true, true, true, true), list);
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createVE(employee).concat(employee.lastName)
                .where(employee.lastName.eq("Cooper"))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            expected = Arrays.asList("0Cooper");
        } else if (SupportedDb.H2.equals(getDatabaseName())) {
            expected = Arrays.asList("FALSECooper");
        } else {
            expected = Arrays.asList("falseCooper");
        }
        assertEquals(expected, list);
    }

    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createVE(employee).concat("-")
                .where(employee.lastName.eq("Cooper"))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            expected = Arrays.asList("0-");
        } else if (SupportedDb.H2.equals(getDatabaseName())) {
            expected = Arrays.asList("FALSE-");
        } else {
            expected = Arrays.asList("false-");
        }
        assertEquals(expected, list);
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.concat(createVE(employee))
                .where(employee.lastName.eq("Cooper"))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            expected = Arrays.asList("Cooper0");
        } else if (SupportedDb.H2.equals(getDatabaseName())) {
            expected = Arrays.asList("CooperFALSE");
        } else {
            expected = Arrays.asList("Cooperfalse");
        }
        assertEquals(expected, list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Employee employee = new Employee();
        final MyDual myDual = new MyDual();
        final List<String> list = myDual.dummy.where(createVE(employee).contains(false)).list(getEngine());
        assertEquals(Arrays.asList("X"), list);
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createVE(employee).countDistinct().list(getEngine());
        assertEquals(Arrays.asList(2), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createVE(employee).count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.orderBy(createVE(employee).desc(), employee.firstName).list(getEngine());
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret", "James"), list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).distinct().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true), list);
    }

    @Override
    public void test_div_Factor() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).div(employee.salary)
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(0.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '*' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean / double precision
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_div_Number() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).div(3.0)
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(0.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '*' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean / numeric
            // org.h2.jdbc.JdbcSQLException: Feature not supported: "BOOLEAN /"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = employee.salary.div(createVE(employee))
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(3000.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '*' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean / double precision
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).eq(false)).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_eq_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).eq(employee.firstName.eq("James").asValue())).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).eq(employee.firstName.eq("James").asValue())).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).exceptAll(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(false, true, true), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).exceptAll(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(false, true, true), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).exceptDistinct(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(false), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).exceptDistinct(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(false), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).except(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(false), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).except(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(false), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Employee employee = new Employee();
        final MyDual myDual = new MyDual();
        final List<String> list = myDual.dummy.where(createVE(employee).exists()).list(getEngine());
        assertEquals(Arrays.asList("X"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).forReadOnly().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true, true, true, true), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true, true, true, true), list);
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).ge(true)).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).ge(employee.firstName.eq("James").asValue()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).ge(employee.firstName.eq("James").asValue()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).gt(true)).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(), list);
    }

    @Override
    public void test_gt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).gt(employee.firstName.eq("James").asValue()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_gt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).gt(employee.firstName.eq("James").asValue()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createVE(employee).in(department.manager().firstName.ne("Bill").asValue()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final Employee other = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).in(true, false))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createVE(employee).in(department.manager().firstName.ne("Bill").asValue()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createVE(employee).eq(department.manager().firstName.eq("Bill").asValue().all()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_any_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createVE(employee).ne(department.manager().firstName.ne("Bill").asValue().any()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createVE(employee).eq(department.manager().firstName.eq("James").asValue().some()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).intersectAll(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(true, true), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).intersectAll(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(true, true), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).exceptDistinct(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(false), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).exceptDistinct(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(false), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).intersect(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(true), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<Boolean> list = createVE(employee).intersect(createVE(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(true), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractValueExpression<Boolean> valueExpression = employee.department().deptName.eq("HR").asValue();
        final List<String> list = employee.lastName.where(valueExpression.isNotNull()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractValueExpression<Boolean> valueExpression = employee.department().deptName.eq("HR").asValue();
        final List<String> list = employee.lastName.where(valueExpression.isNull()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Pair<Boolean, String>> list = createVE(employee)
                .label(label)
                .pair(employee.firstName)
                .orderBy(label, employee.firstName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(false, "James"),
                Pair.make(true, "Alex"),
                Pair.make(true, "Bill"),
                Pair.make(true, "James"),
                Pair.make(true, "Margaret")), list);
    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).le(true)).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).le(employee.firstName.eq("James").asValue()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    @Override
    public void test_le_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).le(employee.firstName.eq("James").asValue()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final String pattern;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                pattern = "0%";
            } else if (SupportedDb.H2.equals(getDatabaseName())) {
                pattern = "FA%";
            } else {
                pattern = "fa%";
            }
            final List<String> list = employee.lastName
                    .where(createVE(employee).like(pattern))
                    .orderBy(employee.lastName)
                    .list(getEngine());
                assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean ~~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final String pattern;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            pattern = "0%";
        } else if (SupportedDb.H2.equals(getDatabaseName())) {
            pattern = "FA%";
        } else {
            pattern = "fa%";
        }
        final Employee employee = new Employee();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set(pattern))).execute(getEngine());

        try {
            final List<String> list = employee.lastName
                    .where(createVE(employee).like(insertTable.text.queryValue()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
                assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean ~~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).limit(2, 10).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true, true, true, true), list);
    }

    @Override
    public void test_countRows_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createVE(employee).countRows().list(getEngine());
        assertEquals(Arrays.asList(5), list);

    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).lt(true)).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_lt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).lt(employee.firstName.eq("James").asValue()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).lt(employee.firstName.eq("James").asValue()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createVE(employee).map(CoreMappers.STRING).list(getEngine());
        Collections.sort(list);
            if (getDatabaseName().equals(SupportedDb.MYSQL)) {
                assertEquals(Arrays.asList("0", "1", "1", "1", "1"), list);
            } else if (getDatabaseName().equals(SupportedDb.POSTGRESQL)) {
                assertEquals(Arrays.asList("f", "t", "t", "t", "t"), list);
            } else if (getDatabaseName().equals(SupportedDb.H2)) {
                assertEquals(Arrays.asList("FALSE", "TRUE", "TRUE", "TRUE", "TRUE"), list);
            } else {
                assertEquals(Arrays.asList("false", "true", "true", "true", "true"), list);
            }
    }

    @Override
    public void test_max_() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Boolean> list = createVE(employee).max().list(getEngine());
            assertEquals(Arrays.asList(true), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function max(boolean) does not exist
            expectSQLException(e, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_min_() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Boolean> list = createVE(employee).min().list(getEngine());
            assertEquals(Arrays.asList(false), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function min(boolean) does not exist
            expectSQLException(e, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_mult_Factor() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).mult(employee.salary)
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(0.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '*' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean * double precision
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_mult_Number() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).mult(3.0)
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(0.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '*' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean * numeric
            // org.h2.jdbc.JdbcSQLException: Feature not supported: "BOOLEAN *"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = employee.salary.mult(createVE(employee))
                    .where(employee.lastName.eq("Cooper"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(0.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '*' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean * double precision
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createVE(employee).ne(true)).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_ne_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).ne(employee.firstName.eq("James").asValue()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).ne(employee.firstName.eq("James").asValue()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).notIn(redwood.firstName.ne("Margaret").asValue().where(redwood.lastName.eq("Redwood"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).notIn(false))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final List<String> list = employee.lastName
                .where(createVE(employee).notIn(redwood.firstName.ne("Margaret").asValue().where(redwood.lastName.eq("Redwood"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final String pattern;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                pattern = "1%";
            } else if (SupportedDb.H2.equals(getDatabaseName())) {
                pattern = "TR%";
            } else {
                pattern = "tr%";
            }

            final List<String> list = employee.lastName
                    .where(createVE(employee).notLike(pattern))
                    .orderBy(employee.lastName)
                    .list(getEngine());
                assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean ~~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final String pattern;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            pattern = "1%";
        } else if (SupportedDb.H2.equals(getDatabaseName())) {
            pattern = "TR%";
        } else {
            pattern = "tr%";
        }
        final Employee employee = new Employee();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set(pattern))).execute(getEngine());

        try {
            final List<String> list = employee.lastName
                    .where(createVE(employee).notLike(insertTable.text.queryValue()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
                assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean ~~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        try {
            final Employee employee = new Employee();
            final AbstractValueExpression<Boolean> valueExpression = employee.department().deptName.eq("HR").asValue();
            final List<String> list = employee.firstName.orderBy(valueExpression.nullsFirst(), employee.firstName).list(getEngine());
            assertEquals(Arrays.asList("James", "Alex", "James", "Bill", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS LAST
                expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        try {
            final Employee employee = new Employee();
            final AbstractValueExpression<Boolean> valueExpression = employee.department().deptName.eq("HR").asValue();
            final List<String> list = employee.firstName.orderBy(valueExpression.nullsLast(), employee.firstName).list(getEngine());
            assertEquals(Arrays.asList("Alex", "James", "Bill", "Margaret", "James"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS LAST
                expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        // what is the opposite to Boolean? many databases may not support it
        // but mysql allows it: - FALSE == FALSE; - TRUE == TRUE (conversion to 0/1, apply '-', convert back to Boolean -
        // everything not zero is true (like in C)
        // in H2 opposite of Boolean is equivalent of negate
        try {
            final Employee employee = new Employee();
            final List<Pair<Boolean, String>> list = createVE(employee).opposite().pair(employee.lastName)
                    .where(employee.firstName.eq("James"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            final List<Pair<Boolean, String>> expected;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                expected = Arrays.asList(Pair.make(false, "Cooper"), Pair.make(true, "First"));
            } else {
                expected = Arrays.asList(Pair.make(true, "Cooper"), Pair.make(false, "First"));
            }
            assertEquals(expected, list);
        } catch (SQLException e) {
            // Apache Derby: ERROR 42X37: The unary '-' operator is not allowed on the 'BOOLEAN' type.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: - boolean
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Boolean, String>> list = employee.firstName.eq("Bill").then(Params.p(false)).orElse(createVE(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(false, "Cooper"),
                Pair.make(true, "First"),
                Pair.make(false, "March"),
                Pair.make(true, "Pedersen"),
                Pair.make(true, "Redwood")), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.orderBy(createVE(employee), employee.firstName).list(getEngine());
        assertEquals(Arrays.asList("James", "Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).orderBy(employee.lastName.desc()).list(getEngine());
        assertEquals(Arrays.asList(true, true, true, true, false), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Boolean,String>> list = createVE(employee).pair(employee.lastName).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(false, "Cooper"),
                Pair.make(true, "First"),
                Pair.make(true, "March"),
                Pair.make(true, "Pedersen"),
                Pair.make(true, "Redwood")), list);
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Boolean>> list = employee.lastName.pair(createVE(employee)).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", false),
                Pair.make("First", true),
                Pair.make("March", true),
                Pair.make("Pedersen", true),
                Pair.make("Redwood", true)), list);
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final AbstractValueExpression<Boolean> ve = createVE(employee);
        final DynamicParameter<Boolean> param = ve.param();
        final SelectStatement<String> query = employee.lastName.where(ve.eq(param))
                .orderBy(employee.lastName);
        param.setValue(false);
        assertEquals(Arrays.asList("Cooper"), query.list(getEngine()));
        param.setValue(true);
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), query.list(getEngine()));
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final AbstractValueExpression<Boolean> ve = createVE(employee);
        final DynamicParameter<Boolean> param = ve.param(false);
        final SelectStatement<String> query = employee.lastName.where(ve.eq(param))
                .orderBy(employee.lastName);
        assertEquals(Arrays.asList("Cooper"), query.list(getEngine()));
    }

    @Override
    public void test_positionOf_String() throws Exception {
        try {
            final String pattern;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                pattern = "0";
            } else if (SupportedDb.H2.equals(getDatabaseName())) {
                pattern = "F";
            } else {
                pattern = "f";
            }
            final Employee employee = new Employee();
            final List<Integer> list = createVE(employee).positionOf(pattern).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // Apache Derby : java.sql.SQLException: Java exception: ': java.lang.NullPointerException'.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.position(boolean, character varying) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        try {
            final String pattern;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                pattern = "0";
            } else if (SupportedDb.H2.equals(getDatabaseName())) {
                pattern = "FALSE";
            } else {
                pattern = "f";
            }
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set(pattern))).execute(getEngine());
            final Employee employee = new Employee();
            final List<Integer> list = insertTable.text.queryValue().positionOf(createVE(employee)).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // Apache Derby :  ERROR 42884: No authorized routine named 'LOCATE' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.position(character varying, boolean) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        try {
            final String pattern;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                pattern = "0";
            } else if (SupportedDb.H2.equals(getDatabaseName())) {
                pattern = "F";
            } else {
                pattern = "f";
            }
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set(pattern))).execute(getEngine());
            final Employee employee = new Employee();
            final List<Integer> list = createVE(employee).positionOf(insertTable.text.queryValue()).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // Apache Derby :  ERROR 42884: No authorized routine named 'LOCATE' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.position(boolean, character varying) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_queryValue_() throws Exception {
        final MyDual myDual = new MyDual();
        final Department department = new Department();
        final AbstractValueExpression<Boolean> ve = myDual.dummy.eq("X").asValue();
        final List<Pair<Boolean, String>> list = ve.queryValue().pair(department.deptName)
                .orderBy(department.deptName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(true, "DEV"), Pair.make(true, "HR")), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> expected = new ArrayList<>(Arrays.asList(false, true, true, true, true));
        createVE(employee).scroll(getEngine(), new Callback<Boolean>() {
            @Override
            public boolean iterate(final Boolean aBoolean) throws SQLException {
                assertTrue(expected.toString(), expected.remove(aBoolean));
                return true;
            }
        });
        assertTrue(expected.toString(), expected.isEmpty());
    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<Boolean> list = createVE(employee).selectAll().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true, true, true, true), list);
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("a"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("b"))).execute(getEngine());

        final AbstractValueExpression<Boolean> ve = insertTable.text.eq("a").asValue();
        insertTable.update(insertTable.active.set(ve)).execute(getEngine());

        final List<Pair<String, Boolean>> list = insertTable.text.pair(insertTable.active).orderBy(insertTable.text)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make("a", true), Pair.make("b", false)), list);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = createVE(employee).showQuery(getEngine().getDialect());
        final String expected;
        if (SupportedDb.POSTGRESQL.equals(getDatabaseName()) || SupportedDb.H2.equals(getDatabaseName())) {
            expected = "SELECT T0.dept_id IS NOT NULL AS C0 FROM employee AS T0";
        } else {
            expected = "SELECT(T0.dept_id IS NOT NULL) AS C0 FROM employee AS T0";
        }
        assertSimilar(expected, sql);
    }

    @Override
    public void test_sub_Number() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).sub(0.5)
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(0.5, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '-' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean - numeric
            // org.h2.jdbc.JdbcSQLException: Feature not supported: "BOOLEAN -"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = employee.salary.sub(createVE(employee))
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(2999.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '-' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean - double precision
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_sub_Term() throws Exception {
        // many dababases do not support
        try {
            final Employee employee = new Employee();
            final List<Number> list = createVE(employee).sub(employee.salary)
                    .where(employee.lastName.eq("First"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(-2999.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y95: The '-' operator with a left operand type of 'BOOLEAN' and a right operand type of 'DOUBLE' is not supported
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: boolean - double precision
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(1))).execute(getEngine());

        try {
            final String expected;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                expected = "0";
            } else if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = "FALSE";
            } else {
                expected = "f";
            }
            final Employee employee = new Employee();
            final List<String> list = createVE(employee).substring(insertTable.payload.queryValue()).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList(expected), list);
        } catch (SQLException e) {
            // Apache Derby : ERROR 42X25: The 'SUBSTR' function is not allowed on the 'BOOLEAN' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(boolean, integer) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }

    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(1))).execute(getEngine());

        try {
            final String expected;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                expected = "0";
            } else if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = "F";
            } else {
                expected = "f";
            }
            final Employee employee = new Employee();
            final List<String> list = createVE(employee)
                    .substring(insertTable.payload.queryValue(), insertTable.payload.queryValue())
                    .where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList(expected), list);
        } catch (SQLException e) {
            // Apache Derby : ERROR 42X25: The 'SUBSTR' function is not allowed on the 'BOOLEAN' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(boolean, integer, integer) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.substring(createVE(employee)).where(employee.lastName.eq("First"))
                    .list(getEngine());
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // Apache Derby :  ERROR 42884: No authorized routine named 'SUBSTR' of type 'FUNCTION' having compatible arguments was found.
            //org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(character varying, boolean) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.substring(createVE(employee), Params.p(2)).where(employee.lastName.eq("First"))
                    .list(getEngine());
            assertEquals(Arrays.asList("Fi"), list);
        } catch (SQLException e) {
            // Apache Derby :  ERROR 42884: No authorized routine named 'SUBSTR' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(character varying, boolean, integer) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.substring(Params.p(2), createVE(employee)).where(employee.lastName.eq("First"))
                    .list(getEngine());
            assertEquals(Arrays.asList("i"), list);
        } catch (SQLException e) {
            // Apache Derby :  ERROR 42884: No authorized routine named 'SUBSTR' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(character varying, integer, boolean) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_int() throws Exception {
        try {
            final String expected;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                expected = "0";
            } else if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = "FALSE";
            } else {
                expected = "f";
            }
            final Employee employee = new Employee();
            final List<String> list = createVE(employee).substring(1).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList(expected), list);
        } catch (SQLException e) {
            // Apache Derby : ERROR 42X25: The 'SUBSTR' function is not allowed on the 'BOOLEAN' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(character varying, integer, boolean) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_int_int() throws Exception {
        try {
            final String expected;
            if (SupportedDb.MYSQL.equals(getDatabaseName())) {
                expected = "0";
            } else if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = "F";
            } else {
                expected = "f";
            }
            final Employee employee = new Employee();
            final List<String> list = createVE(employee).substring(1, 1).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList(expected), list);
        } catch (SQLException e) {
            // Apache Derby : ERROR 42X25: The 'SUBSTR' function is not allowed on the 'BOOLEAN' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(boolean, integer, integer) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Number> list = createVE(employee).sum().list(getEngine());
            assertEquals(1, list.size());
            assertEquals(4.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42Y22: Aggregate SUM cannot operate on type BOOLEAN.
            // org.postgresql.util.PSQLException: ERROR: function sum(boolean) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Boolean, String>> list = employee.firstName.ne("Bill").then(createVE(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(false, "Cooper"),
                Pair.make(true, "First"),
                Pair.make(null, "March"),
                Pair.make(true, "Pedersen"),
                Pair.make(true, "Redwood")), list);
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Boolean> list = createVE(employee).unionAll(createVE(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true, true, true, true, true, true), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Boolean> list = createVE(employee).unionAll(createVE(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true, true, true, true, true, true), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Boolean> list = createVE(employee).unionDistinct(createVE(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true), list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Boolean> list = createVE(employee).unionDistinct(createVE(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Boolean> list = createVE(employee).union(createVE(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true), list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Boolean> list = createVE(employee).union(createVE(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(false, true), list);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
                final List<Boolean> list = createVE(employee).where(employee.lastName.eq("Cooper")).list(getEngine());
                assertEquals(Arrays.asList(false), list);
    }

}
