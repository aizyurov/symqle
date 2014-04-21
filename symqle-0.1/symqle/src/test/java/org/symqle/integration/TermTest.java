package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.integration.model.One;
import org.symqle.sql.AbstractQuerySpecificationScalar;
import org.symqle.sql.AbstractTerm;
import org.symqle.sql.AbstractValueExpressionPrimary;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Label;
import org.symqle.sql.Mappers;
import org.symqle.testset.AbstractTermTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class TermTest extends AbstractIntegrationTestBase implements AbstractTermTestSet {


    private AbstractTerm<Number> createTerm(final Employee employee) {
        return employee.salary.mult(-1);
    }

    @Override
    public void test_add_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createTerm(employee).add(3000.0)
                .map(Mappers.DOUBLE)
                .list(getEngine());
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(0.0, 0.0, 1000.0, 1000.0, 1500.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createTerm(employee).add(employee.salary.mult(2))
                .map(Mappers.DOUBLE)
                .list(getEngine());
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_add_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createTerm(employee).add(employee.salary.mult(2))
                .map(Mappers.DOUBLE)
                .list(getEngine());
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.sub(3000)
                        .in(employee.salary.add(100).asInValueList().append(createTerm(employee))))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);

    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.sub(3000)
                        .in(createTerm(employee).asInValueList().append(employee.salary.add(100))))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_asBoolean_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("zero"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(-1).also(insertTable.text.set("one"))).execute(getEngine());
            final List<String> list = insertTable.text.where(insertTable.id.mult(-1).asBoolean()).list(getEngine());
            assertEquals(Arrays.asList("one"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: cannot cast type numeric to boolean
            // ERROR 42846: Cannot convert types 'INTEGER' to 'BOOLEAN'
            expectSQLException(e, SupportedDb.POSTGRESQL, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createTerm(employee)
                .map(Mappers.DOUBLE)
                .orderBy(createTerm(employee).asc()).list(getEngine());
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createTerm(employee).avg().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(-2300.0, list.get(0).doubleValue());
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createTerm(employee).cast("DECIMAL(7,2)")
                .map(Mappers.DOUBLE)
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_charLength_() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = createTerm(employee).charLength()
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(1, list.size());
            int value = list.get(0);
            // different databases return different values
            assertTrue("Not expected: " + value, value > 4 && value < 10);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function char_length(double precision) does not exist
            expectSQLException(e, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createTerm(employee)
                    .collate(validCollationNameForNumber())
                    .concat(" marsian $")
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("-1500 marsian $", "-3000 marsian $", "-2000 marsian $", "-2000 marsian $", "-3000 marsian $"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE" at line 1, column 23.
            // org.postgresql.util.PSQLException: ERROR: collations are not supported by type double precision
            // org.h2.jdbc.JdbcSQLException: Syntax error in SQL statement
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createTerm(employee).list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createTerm(employee).concat(employee.lastName)
                    .orderBy(employee.lastName)
                    .list(getEngine());
            final List<String> expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = Arrays.asList("-1500.0Cooper", "-3000.0First", "-2000.0March", "-2000.0Pedersen", "-3000.0Redwood");
            } else {
                expected = Arrays.asList("-1500Cooper", "-3000First", "-2000March", "-2000Pedersen", "-3000Redwood");
            }
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createTerm(employee).concat(" marsian dollars")
                    .orderBy(employee.lastName)
                    .list(getEngine());
            final List<String> expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = Arrays.asList("-1500.0 marsian dollars", "-3000.0 marsian dollars", "-2000.0 marsian dollars", "-2000.0 marsian dollars", "-3000.0 marsian dollars");
            } else {
                expected = Arrays.asList("-1500 marsian dollars", "-3000 marsian dollars", "-2000 marsian dollars", "-2000 marsian dollars", "-3000 marsian dollars");
            }
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.concat(createTerm(employee))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            final List<String> expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = Arrays.asList("Cooper-1500.0", "First-3000.0", "March-2000.0", "Pedersen-2000.0", "Redwood-3000.0");
            } else {
                expected = Arrays.asList("Cooper-1500", "First-3000", "March-2000", "Pedersen-2000", "Redwood-3000");
            }
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createTerm(employee).contains(-3000.0))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createTerm(employee).countDistinct().list(getEngine());
        assertEquals(Arrays.asList(3), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createTerm(employee).count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createTerm(employee)
                .map(Mappers.DOUBLE)
                .orderBy(createTerm(employee).desc()).list(getEngine());
        assertEquals(Arrays.asList(-1500.0, -2000.0, -2000.0, -3000.0, -3000.0), list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createTerm(employee).distinct().list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_div_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createTerm(employee).div(createTerm(employee).div(employee.salary)).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_div_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createTerm(employee).div(0.5).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(-6000.0, -6000.0, -4000.0, -4000.0, -3000.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.mult(employee.salary).div(createTerm(employee)).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createTerm(employee).eq(-1500.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_eq_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4500.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(createTerm(employee)
                .eq(subqueryValue))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4500.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(subqueryValue
                .eq(createTerm(employee)))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).exceptAll(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-2000.0, -2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).exceptAll(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-2000.0, -2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).exceptDistinct(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).exceptDistinct(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).except(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).except(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createTerm(employee).exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createTerm(employee).forReadOnly().list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createTerm(employee).forUpdate().list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createTerm(employee).ge(-2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_ge_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4500.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(createTerm(employee)
                .ge(subqueryValue))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_ge_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4500.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(subqueryValue
                .ge(createTerm(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createTerm(employee).gt(-2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_gt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4800.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(createTerm(employee)
                .gt(subqueryValue))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_gt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4500.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(subqueryValue
                .gt(createTerm(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
            final List<String> list = employee.lastName
                    .where(createTerm(employee).in(createTerm(department.manager())))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
            final List<String> list = employee.lastName
                    .where(createTerm(employee).eq(createTerm(department.manager()).all()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_any_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
            final List<String> list = employee.lastName
                    .where(createTerm(employee).ne(createTerm(department.manager()).any()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
            final List<String> list = employee.lastName
                    .where(createTerm(employee).eq(createTerm(department.manager()).some()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createTerm(employee).in(-2000.0, -1500.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
            final List<String> list = employee.lastName
                    .where(createTerm(employee).in(createTerm(department.manager())))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).intersectAll(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).intersectAll(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).intersectDistinct(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).intersectDistinct(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).intersect(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = toListOfDouble(createTerm(employee).intersect(createTerm(department.manager())).list(getEngine()));
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractTerm<Number> term = employee.deptId.mult(2);
        final List<String> list = employee.lastName.where(term.isNotNull()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractTerm<Number> term = employee.deptId.mult(2);
        final List<String> list = employee.lastName.where(term.isNull()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Number> numberList = createTerm(employee).label(label).orderBy(label).list(getEngine());
        final List<Double> list = toListOfDouble(numberList);
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createTerm(employee).le(-2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4500.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(createTerm(employee)
                .le(subqueryValue))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4500.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(subqueryValue
                .le(createTerm(employee)))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createTerm(employee).like("-2%"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: double precision ~~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        try {
            final AbstractQuerySpecificationScalar<String> subQuery =
                    redwood.salary.opposite().substring(1, 2).concat("%").where(redwood.lastName.eq("Redwood"));
            final List<String> list = employee.lastName.where(createTerm(employee).like(subQuery.queryValue()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: double precision ~~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createTerm(employee).limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createTerm(employee).limit(2, 10).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createTerm(employee).list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_countRows_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createTerm(employee).countRows().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createTerm(employee).lt(-2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_lt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4500.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(createTerm(employee)
                .lt(subqueryValue))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4800.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(subqueryValue
                .lt(createTerm(employee)))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createTerm(employee)
                .map(Mappers.DOUBLE)
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createTerm(employee).max().list(getEngine()));
        assertEquals(Arrays.asList(-1500.0), list);
    }

    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createTerm(employee).min().list(getEngine()));
        assertEquals(Arrays.asList(-3000.0), list);
    }

    @Override
    public void test_mult_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createTerm(employee).mult(employee.salary.div(createTerm(employee))).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_mult_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createTerm(employee).mult(2).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(-6000.0, -6000.0, -4000.0, -4000.0, -3000.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.div(createTerm(employee)).mult(createTerm(employee)).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createTerm(employee).ne(-1500.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ne_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4500.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(createTerm(employee)
                .ne(subqueryValue))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final AbstractValueExpressionPrimary<Number> subqueryValue =
                redwood.salary.sub(4500.0).where(redwood.lastName.eq("Redwood")).queryValue();
        final List<String> list = employee.lastName.where(subqueryValue
                .ne(createTerm(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
            final List<String> list = employee.lastName
                    .where(createTerm(employee).notIn(createTerm(department.manager())))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createTerm(employee).notIn(-2000.0, -1500.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
            final List<String> list = employee.lastName
                    .where(createTerm(employee).notIn(createTerm(department.manager())))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createTerm(employee).notLike("-2%"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: double precision !~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        try {
            final AbstractQuerySpecificationScalar<String> subQuery =
                    redwood.salary.opposite().substring(1, 2).concat("%").where(redwood.lastName.eq("Redwood"));
            final List<String> list = employee.lastName.where(createTerm(employee).notLike(subQuery.queryValue()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: double precision ~~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(1))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.payload.setNull())).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.payload.set(3))).execute(getEngine());
            final List<Integer> list = insertTable.id
                    .orderBy(insertTable.payload.mult(2).nullsFirst())
                    .list(getEngine());
            assertEquals(Arrays.asList(2, 1, 3), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(1))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.payload.setNull())).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.payload.set(3))).execute(getEngine());
            final List<Integer> list = insertTable.id
                    .orderBy(insertTable.payload.mult(2).nullsLast())
                    .list(getEngine());
            assertEquals(Arrays.asList(1, 3, 2), list);
        } catch (SQLException e) {
            // mysql does not support NULLS LAST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createTerm(employee).opposite().list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Number, String>> list = employee.deptId.isNull().then(employee.salary.mult(2)).orElse(createTerm(employee))
                .pair(employee.lastName)
                .where(employee.firstName.eq("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(2, list.size());
        final Pair<Number, String> cooper = list.get(0);
        assertEquals(3000.0, cooper.first().doubleValue());
        assertEquals("Cooper", cooper.second());
        final Pair<Number, String> first = list.get(1);
        assertEquals(-3000.0, first.first().doubleValue());
        assertEquals("First", first.second());
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createTerm(employee)
                .map(Mappers.DOUBLE)
                .orderBy(createTerm(employee)).list(getEngine());
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createTerm(employee)
                .orderBy(createTerm(employee).asc()).list(getEngine());
        assertEquals(5, list.size());
        assertEquals(-3000.0, list.get(0).doubleValue());
        assertEquals(-1500.0, list.get(4).doubleValue());
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Number, String>> list = createTerm(employee).pair(employee.lastName).where(employee.retired.asBoolean()).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(-1500, list.get(0).first().intValue());
        assertEquals("Cooper", list.get(0).second());
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Number>> list = employee.lastName.pair(createTerm(employee)).where(employee.retired.asBoolean()).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(-1500, list.get(0).second().intValue());
        assertEquals("Cooper", list.get(0).first());
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final AbstractTerm<Number> term = createTerm(employee);
        final DynamicParameter<Number> param = term.param();
        param.setValue(-1500.0);
        final List<String> list = employee.lastName.where(term.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final AbstractTerm<Number> term = createTerm(employee);
        final DynamicParameter<Number> param = term.param(-1500.0);
        final List<String> list = employee.lastName.where(term.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_positionOf_String() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = createTerm(employee).positionOf("500")
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            assertEquals(Arrays.asList(3), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.position(double precision, character varying) does not exist
            // apache Derby: ava.sql.SQLException: Java exception: ': java.lang.NullPointerException'.
            expectSQLException(e, SupportedDb.POSTGRESQL, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = createTerm(employee).positionOf(employee.salary.substring(2))
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            assertEquals(Arrays.asList(3), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.position(double precision, character varying) does not exist
            // apache Derby: ava.sql.SQLException: Java exception: ': java.lang.NullPointerException'.
            expectSQLException(e, SupportedDb.POSTGRESQL, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = employee.salary.positionOf(employee.salary.div(3))
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            System.out.println(employee.salary.div(3)
                    .where(employee.lastName.eq("Cooper")).list(getEngine()));
            System.out.println(createTerm(employee)
                                .where(employee.lastName.eq("Cooper")).list(getEngine()));
            assertEquals(Arrays.asList(2), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.position(double precision, character varying) does not exist
            // apache Derby: ava.sql.SQLException: Java exception: ': java.lang.NullPointerException'.
            expectSQLException(e, SupportedDb.POSTGRESQL, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_queryValue_() throws Exception {
        final List<Pair<Integer, String>> list = new One().id.opposite().queryValue().pair(new MyDual().dummy)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(-1, "X")), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> expected = new ArrayList<>(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0));
        createTerm(employee).scroll(getEngine(), new Callback<Number>() {
            @Override
            public boolean iterate(final Number number) throws SQLException {
                assertTrue(expected.toString(), expected.remove(number.doubleValue()));
                return true;
            }
        });
        assertTrue(expected.toString(), expected.isEmpty());
    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createTerm(employee).selectAll().list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(3))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.payload.set(5))).execute(getEngine());

        insertTable.update(insertTable.payload.set(insertTable.id.mult(insertTable.payload).map(Mappers.INTEGER)))
            .execute(getEngine());

        final List<Pair<Integer, Integer>> list = insertTable.id.pair(insertTable.payload)
                .orderBy(insertTable.id).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, 3), Pair.make(2, 10)), list);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = createTerm(employee).showQuery(getEngine().getDialect());
        assertSimilar("SELECT T0.salary * ? AS C0 FROM employee AS T0", sql);
    }

    @Override
    public void test_sub_Number() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<Double> list = createTerm(employee).sub(500.0)
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        final List<Double> expected = Arrays.asList(-3500.0, -3500.0, -2500.0, -2500.0, -2000.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<Double> list = employee.salary.mult(2).opposite().sub(createTerm(employee))
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        final List<Double> expected = Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_sub_Term() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<Double> list = createTerm(employee).sub(employee.salary.mult(2).opposite())
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(12345)))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(3)
                .also(insertTable.payload.set(67890)))
                .execute(getEngine());
        try {
            final List<String> list = insertTable.payload.mult(1).substring(insertTable.id)
                    .orderBy(insertTable.id)
                    .list(getEngine());

            assertEquals(Arrays.asList("2345", "890"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(numeric, integer) does not exist
            // Apache Derby: ERROR 42X25: The 'SUBSTR' function is not allowed on the 'INTEGER' type.
            expectSQLException(e, SupportedDb.POSTGRESQL, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(12345)))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(3)
                .also(insertTable.payload.set(67890)))
                .execute(getEngine());
        try {
            final List<String> list = insertTable.payload.mult(1).substring(insertTable.id, insertTable.id.sub(1))
                    .orderBy(insertTable.id)
                    .list(getEngine());

            assertEquals(Arrays.asList("2", "89"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(numeric, integer) does not exist
            // Apache Derby: ERROR 42X25: The 'SUBSTR' function is not allowed on the 'INTEGER' type.
            expectSQLException(e, SupportedDb.POSTGRESQL, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(3))
                .also(insertTable.text.set("abcdefg"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(2))
                .also(insertTable.text.set("ijklmnop"))).execute(getEngine());
        final List<String> list = insertTable.text.substring(insertTable.id.mult(insertTable.payload))
                .orderBy(insertTable.id)
                .list(getEngine());

        assertEquals(Arrays.asList("cdefg", "lmnop"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(3))
                .also(insertTable.text.set("abcdefg"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(2))
                .also(insertTable.text.set("ijklmnop"))).execute(getEngine());
        final List<String> list = insertTable.text
                .substring(insertTable.id.mult(insertTable.payload),
                        insertTable.payload)
                .orderBy(insertTable.id)
                .list(getEngine());

        assertEquals(Arrays.asList("cde", "lm"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(3))
                .also(insertTable.text.set("abcdefg"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(2))
                .also(insertTable.text.set("ijklmnop"))).execute(getEngine());
        final List<String> list = insertTable.text
                .substring(insertTable.payload, insertTable.id.mult(insertTable.payload))
                .orderBy(insertTable.id)
                .list(getEngine());

        assertEquals(Arrays.asList("cde", "jklm"), list);
    }

    @Override
    public void test_substring_int() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(12345)))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(3)
                .also(insertTable.payload.set(67890)))
                .execute(getEngine());
        try {
            final List<String> list = insertTable.payload.mult(1).substring(3)
                    .orderBy(insertTable.id)
                    .list(getEngine());

            assertEquals(Arrays.asList("345", "890"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(numeric, integer) does not exist
            // Apache Derby: ERROR 42X25: The 'SUBSTR' function is not allowed on the 'INTEGER' type.
            expectSQLException(e, SupportedDb.POSTGRESQL, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_substring_int_int() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(12345)))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(3)
                .also(insertTable.payload.set(67890)))
                .execute(getEngine());
        try {
            final List<String> list = insertTable.payload.mult(1).substring(3, 2)
                    .orderBy(insertTable.id)
                    .list(getEngine());

            assertEquals(Arrays.asList("34", "89"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(numeric, integer) does not exist
            // Apache Derby: ERROR 42X25: The 'SUBSTR' function is not allowed on the 'INTEGER' type.
            expectSQLException(e, SupportedDb.POSTGRESQL, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createTerm(employee).sum().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(-11500.0, list.get(0).doubleValue());
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Number, String>> list = employee.deptId.isNotNull()
                .then(createTerm(employee))
                .orElse(employee.salary.mult(2))
                .pair(employee.lastName)
                .where(employee.firstName.eq("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(2, list.size());
        final Pair<Number, String> cooper = list.get(0);
        assertEquals(3000.0, cooper.first().doubleValue());
        assertEquals("Cooper", cooper.second());
        final Pair<Number, String> first = list.get(1);
        assertEquals(-3000.0, first.first().doubleValue());
        assertEquals("First", first.second());
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = toListOfDouble(createTerm(employee).unionAll(createTerm(department.manager())).list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = toListOfDouble(createTerm(employee).unionAll(createTerm(department.manager())).list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = toListOfDouble(createTerm(employee).unionDistinct(createTerm(department.manager())).list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = toListOfDouble(createTerm(employee).unionDistinct(createTerm(department.manager())).list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = toListOfDouble(createTerm(employee).union(createTerm(department.manager())).list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = toListOfDouble(createTerm(employee).union(createTerm(department.manager())).list(getEngine()));
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createTerm(employee).where(employee.lastName.eq("Cooper")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(-1500.0, list.get(0).doubleValue());
    }

}
