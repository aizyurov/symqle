package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.integration.model.One;
import org.symqle.sql.AbstractNumericExpression;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Label;
import org.symqle.sql.Mappers;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractNumericExpressionTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class NumericExpressionTest extends AbstractIntegrationTestBase implements AbstractNumericExpressionTestSet {


    private AbstractNumericExpression<Number> createExpression(final Employee employee) {
        return employee.salary.add(100);
    }

    @Override
    public void test_add_Number() throws Exception {
        final Employee employee = new Employee();
        Label l = new Label();
        final List<Double> list = createExpression(employee).add(100.0)
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(1700.0, 2200.0, 2200.0, 3200.0, 3200.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        Label l = new Label();
        final List<Double> list = employee.salary.mult(2).add(createExpression(employee))
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(4600.0, 6100.0, 6100.0, 9100.0, 9100.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_add_Term() throws Exception {
        final Employee employee = new Employee();
        Label l = new Label();
        final List<Double> list = createExpression(employee).add(employee.salary.mult(2))
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(4600.0, 6100.0, 6100.0, 9100.0, 9100.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.map(Mappers.NUMBER).in(
                employee.department().manager().salary.add(100).asInValueList()
                        .append(employee.department().manager().salary.sub(1000.0))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.map(Mappers.NUMBER).in(
                employee.department().manager().salary.sub(1000).asInValueList()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_asBoolean_() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two"))).execute(getEngine());
            final List<String> list = insertTable.text.where(insertTable.id.sub(1).asBoolean()).list(getEngine());
            assertEquals(Arrays.asList("two"), list);
        } catch (SQLException e) {
            // ERROR 42846: Cannot convert types 'INTEGER' to 'BOOLEAN'.
            // org.postgresql.util.PSQLException: ERROR: cannot cast type numeric to boolean
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);

        }

    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createExpression(employee)
                .map(Mappers.DOUBLE)
                .orderBy(createExpression(employee).asc())
                .list(getEngine());
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee).avg().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(2400.0, list.get(0).doubleValue());
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createExpression(employee).cast("DECIMAL(7,2)")
                .map(Mappers.DOUBLE)
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    @Override
    public void test_charLength_() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = createExpression(employee).charLength()
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(1, list.size());
            int value = list.get(0);
            // different databases return different values
            assertTrue("Not expected: " + value, value > 3 && value < 10);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function char_length(double precision) does not exist
            expectSQLException(e, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createExpression(employee)
                    .collate(validCollationNameForNumber())
                    .concat(" marsian $")
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("1600 marsian $", "3100 marsian $", "2100 marsian $", "2100 marsian $", "3100 marsian $"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE"
            // org.postgresql.util.PSQLException: ERROR: collations are not supported by type double precision
            // org.h2.jdbc.JdbcSQLException: Syntax error in SQL statement
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee)
                .compileQuery(getEngine())
                .list();
        final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0));
        for (Number n : list) {
            assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
        }
        assertTrue("Remaining: " + expected, expected.isEmpty());
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createExpression(employee).concat(employee.lastName)
                    .orderBy(employee.lastName)
                    .list(getEngine());
            final List<String> expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = Arrays.asList("1600.0Cooper", "3100.0First", "2100.0March", "2100.0Pedersen", "3100.0Redwood");
            } else {
                expected = Arrays.asList("1600Cooper", "3100First", "2100March", "2100Pedersen", "3100Redwood");
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
            final List<String> list = createExpression(employee).concat(" marsian $")
                    .orderBy(employee.lastName)
                    .list(getEngine());
            final List<String> expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = Arrays.asList("1600.0 marsian $", "3100.0 marsian $", "2100.0 marsian $", "2100.0 marsian $", "3100.0 marsian $");
            } else {
                expected = Arrays.asList("1600 marsian $", "3100 marsian $", "2100 marsian $", "2100 marsian $", "3100 marsian $");
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
            final List<String> list = employee.lastName.concat(createExpression(employee))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            final List<String> expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = Arrays.asList("Cooper1600.0", "First3100.0", "March2100.0", "Pedersen2100.0", "Redwood3100.0");
            } else {
                expected = Arrays.asList("Cooper1600", "First3100", "March2100", "Pedersen2100", "Redwood3100");
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
                .where(createExpression(employee).contains(1600.0))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createExpression(employee).countDistinct().list(getEngine());
        assertEquals(Arrays.asList(3), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createExpression(employee).count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createExpression(employee)
                .map(Mappers.DOUBLE)
                .orderBy(createExpression(employee).desc())
                .list(getEngine());
        assertEquals(Arrays.asList(3100.0, 3100.0, 2100.0, 2100.0, 1600.0), list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee).distinct().list(getEngine());
        final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 3100.0));
        for (Number n : list) {
            assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
        }
        assertTrue("Remaining: " + expected, expected.isEmpty());
    }

    @Override
    public void test_div_Factor() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label l = new Label();
        final List<Double> list = createExpression(employee).div(department.deptId.count().queryValue())
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        final List<Double> expected = Arrays.asList(800.0, 1050.0, 1050.0, 1550.0, 1550.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_div_Number() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<Double> list = createExpression(employee).div(0.5)
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        final List<Double> expected = Arrays.asList(3200.0, 4200.0, 4200.0, 6200.0, 6200.0);
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label l = new Label();
        final List<Double> list = employee.salary.mult(2).div(employee.salary.sub(1000))
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        final List<Double> expected = Arrays.asList(3.0, 3.0, 4.0, 4.0, 6.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).eq(1600.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_eq_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.add(1500).eq(employee.salary.mult(2))).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.mult(2).eq(employee.salary.add(1500))).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).exceptAll(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 2100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).exceptAll(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 2100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).exceptDistinct(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).exceptDistinct(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).except(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).except(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
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
                .where(createExpression(employee).exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee)
                .forReadOnly()
                .list(getEngine());
        final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0));
        for (Number n : list) {
            assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
        }
        assertTrue("Remaining: " + expected, expected.isEmpty());
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee)
                .forUpdate()
                .list(getEngine());
        final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0));
        for (Number n : list) {
            assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
        }
        assertTrue("Remaining: " + expected, expected.isEmpty());
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).ge(2100.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).ge(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(), list);
    }

    @Override
    public void test_ge_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).ge(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).gt(2100.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_gt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).gt(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(), list);
    }

    @Override
    public void test_gt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).gt(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createExpression(employee).in(createExpression(department.manager())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).in(1600.0, 2100.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createExpression(employee).in(createExpression(department.manager())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createExpression(employee).eq(createExpression(department.manager()).all()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_any_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createExpression(employee).ge(createExpression(department.manager()).any()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createExpression(employee).eq(createExpression(department.manager()).some()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).intersectAll(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(3100.0, 3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).intersectAll(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(3100.0, 3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).intersectDistinct(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).intersectDistinct(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).intersect(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).intersect(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.deptId.add(1).isNotNull()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.deptId.add(1).isNull()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Number> list = createExpression(employee).label(label).orderBy(label).list(getEngine());
        final List<Double> doubles = new ArrayList<Double>();
        for (Number n: list) {
            doubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), doubles);

    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).le(2100.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_le_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).le(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).le(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createExpression(employee).like("21%"))
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
        try {
            final List<String> list = employee.lastName.where(createExpression(employee).like(Params.p("21%")))
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
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee)
                .limit(2)
                .list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee)
                .limit(2, 5)
                .list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee)
                .list(getEngine());
        final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0));
        for (Number n : list) {
            assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
        }
        assertTrue("Remaining: " + expected, expected.isEmpty());
    }

    @Override
    public void test_countRows_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createExpression(employee)
                .countRows()
                .list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).lt(2100.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_lt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).lt(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).lt(employee.salary.add(200)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_map_Mapper() throws Exception {
        Employee employee = new Employee();
        final List<Integer> list = createExpression(employee).map(Mappers.INTEGER)
                .orderBy(createExpression(employee))
                .list(getEngine());
        assertEquals(Arrays.asList(1600, 2100, 2100, 3100, 3100), list);
    }

    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee).max().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(3100.0, list.get(0).doubleValue());
    }

    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee).min().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(1600.0, list.get(0).doubleValue());
    }

    @Override
    public void test_mult_Factor() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Double> list = createExpression(employee).mult(employee.salary.div(1000))
                .map(Mappers.DOUBLE)
                .label(label)
                .orderBy(label)
                .list(getEngine());
        assertEquals(Arrays.asList(2400.0, 4200.0, 4200.0, 9300.0, 9300.0), list);
    }

    @Override
    public void test_mult_Number() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Double> list = createExpression(employee).mult(2)
                .map(Mappers.DOUBLE)
                .label(label)
                .orderBy(label)
                .list(getEngine());
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(3200.0, 4200.0, 4200.0, 6200.0, 6200.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Double> list = employee.salary.div(1000).mult(createExpression(employee))
                .map(Mappers.DOUBLE)
                .label(label)
                .orderBy(label)
                .list(getEngine());
        assertEquals(Arrays.asList(2400.0, 4200.0, 4200.0, 9300.0, 9300.0), list);
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).ne(1600.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ne_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.add(1500).ne(employee.salary.mult(2)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.mult(2).ne(employee.salary.add(1500)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createExpression(employee).notIn(createExpression(department.manager())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createExpression(employee).notIn(1600.0, 2100.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createExpression(employee).notIn(createExpression(department.manager())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createExpression(employee).notLike("21%"))
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
        try {
            final List<String> list = employee.lastName.where(createExpression(employee).notLike(Params.p("21").concat("%")))
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
    public void test_nullsFirst_() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("zero")).also(insertTable.payload.setNull()))
                    .execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one")).also(insertTable.payload.set(1)))
                    .execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two")).also(insertTable.payload.set(2)))
                    .execute(getEngine());
            final List<String> list = insertTable.text
                    .orderBy(insertTable.payload.add(1).nullsFirst())
                    .list(getEngine());
            assertEquals(Arrays.asList("zero", "one", "two"), list);
        } catch (SQLException e) {
            // MySQL does not support NULLS FIRST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("zero")).also(insertTable.payload.setNull()))
                    .execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one")).also(insertTable.payload.set(1)))
                    .execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two")).also(insertTable.payload.set(2)))
                    .execute(getEngine());
            final List<String> list = insertTable.text
                    .orderBy(insertTable.payload.add(1).nullsLast())
                    .list(getEngine());
            assertEquals(Arrays.asList("one", "two", "zero"), list);
        } catch (SQLException e) {
            // MySQL does not support NULLS LAST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Double> list = createExpression(employee).opposite()
                .map(Mappers.DOUBLE)
                .label(label)
                .orderBy(label)
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3100.0, -3100.0, -2100.0, -2100.0, -1600.0), list);
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Double> list = employee.salary.lt(1700.0).then(employee.salary.sub(100)).orElse(createExpression(employee))
                .map(Mappers.DOUBLE)
                .label(label)
                .orderBy(label)
                .list(getEngine());
        assertEquals(Arrays.asList(1400.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createExpression(employee)
                .map(Mappers.DOUBLE)
                .orderBy(createExpression(employee))
                .list(getEngine());
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee)
                .orderBy(createExpression(employee).asc())
                .list(getEngine());
        final List<Double> doubles = toListOfDouble(list);
        assertEquals(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0), doubles);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Number, String>> list = createExpression(employee).pair(employee.lastName)
                .where(employee.lastName.eq("Cooper")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals("Cooper", list.get(0).second());
        assertEquals(1600, list.get(0).first().intValue());
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Number>> list = employee.lastName.pair(createExpression(employee))
                .where(employee.lastName.eq("Cooper")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals("Cooper", list.get(0).first());
        assertEquals(1600, list.get(0).second().intValue());
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final AbstractNumericExpression<Number> expression = createExpression(employee);
        final DynamicParameter<Number> param = expression.param();
        param.setValue(1600.0);
        final List<String> list = employee.lastName.where(expression.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final AbstractNumericExpression<Number> expression = createExpression(employee);
        final DynamicParameter<Number> param = expression.param(1600.0);
        final List<String> list = employee.lastName.where(expression.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_positionOf_String() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = createExpression(employee).positionOf("600")
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            assertEquals(Arrays.asList(2), list);
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
            final List<Integer> list = createExpression(employee).positionOf(employee.lastName)
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            assertEquals(Arrays.asList(0), list);
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
            final List<Integer> list = employee.lastName.concat(createExpression(employee)).positionOf(createExpression(employee))
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            assertEquals(Arrays.asList(7), list);
        } catch (SQLException e) {
            // Apache Derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.position(text, double precision) does not exist
            expectSQLException(e, SupportedDb.POSTGRESQL, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_queryValue_() throws Exception {
        final List<Pair<Number, String>> list = new One().id.add(10).queryValue().pair(new MyDual().dummy)
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(11, list.get(0).first().intValue());
        assertEquals("X", list.get(0).second());
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0));
        final int callCount = createExpression(employee)
                .scroll(getEngine(), new Callback<Number>() {
                    @Override
                    public boolean iterate(final Number number) throws SQLException {
                        assertTrue(expected + " does not contain " + number, expected.remove(number.doubleValue()));
                        return true;
                    }
                });
        assertEquals(5, callCount);
        assertTrue("Remaining: " + expected, expected.isEmpty());
    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee)
                .selectAll()
                .list(getEngine());
        final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0));
        for (Number n : list) {
            assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
        }
        assertTrue("Remaining: " + expected, expected.isEmpty());
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one")).also(insertTable.payload.set(1)))
                .execute(getEngine());
        insertTable.update(insertTable.payload.set(insertTable.payload.add(3).map(Mappers.INTEGER)))
                .execute(getEngine());
        assertEquals(Arrays.asList(4), insertTable.payload.list(getEngine()));
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = createExpression(employee).showQuery(getEngine().getDialect());
        assertSimilar("SELECT T0.salary + ? AS C0 FROM employee AS T0", sql);
    }

    @Override
    public void test_sub_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = toListOfDouble(createExpression(employee).sub(500.0).list(getEngine()));
        Collections.sort(list);
        final List<Double> expected = Arrays.asList(1100.0, 1600.0, 1600.0, 2600.0, 2600.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<Double> list = employee.salary.sub(createExpression(employee))
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        final List<Double> expected = Arrays.asList(-100.0, -100.0, -100.0, -100.0, -100.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_sub_Term() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<Double> list = createExpression(employee).sub(employee.salary)
                .map(Mappers.DOUBLE)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        final List<Double> expected = Arrays.asList(100.0, 100.0, 100.0, 100.0, 100.0);
        assertEquals(expected, list);
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = createExpression(employee).substring(employee.lastName.charLength().div(3)).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            final String expected = SupportedDb.H2.equals(getDatabaseName()) ? "600.0" : "600";
            assertEquals(Arrays.asList(expected), list);
        } catch (SQLException e) {
            // ERROR 42X25: The 'SUBSTR' function is not allowed on the 'DOUBLE' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(double precision, numeric) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = createExpression(employee)
                    .substring(employee.lastName.charLength().div(3), employee.lastName.charLength().div(3))
                    .where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList("60"), list);
        } catch (SQLException e) {
            // ERROR 42X25: The 'SUBSTR' function is not allowed on the 'DOUBLE' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(double precision, numeric) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1)
                    .also(insertTable.payload.set(2)
                            .also(insertTable.text.set("abcdef")))).execute(getEngine());
            final List<String> list = insertTable.text.substring(insertTable.payload.add(1)).list(getEngine());
            assertEquals(Arrays.asList("cdef"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1)
                    .also(insertTable.payload.set(2)
                            .also(insertTable.text.set("abcdef")))).execute(getEngine());
            final List<String> list = insertTable.text.substring(insertTable.payload.add(1), insertTable.payload.add(1))
                    .list(getEngine());
            assertEquals(Arrays.asList("cde"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1)
                    .also(insertTable.payload.set(2)
                            .also(insertTable.text.set("abcdef")))).execute(getEngine());
            final List<String> list = insertTable.text.substring(insertTable.payload.add(1), insertTable.payload.add(1))
                    .list(getEngine());
            assertEquals(Arrays.asList("cde"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_int() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = createExpression(employee).substring(2).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            final String expected = SupportedDb.H2.equals(getDatabaseName()) ? "600.0" : "600";
            assertEquals(Arrays.asList(expected), list);
        } catch (SQLException e) {
            // ERROR 42X25: The 'SUBSTR' function is not allowed on the 'DOUBLE' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(double precision, integer) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_int_int() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = createExpression(employee).substring(2, 2).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList("60"), list);
        } catch (SQLException e) {
            // ERROR 42X25: The 'SUBSTR' function is not allowed on the 'DOUBLE' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(double precision, integer) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee).sum().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(12000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Double> list = employee.salary.lt(1700.0).then(employee.salary.sub(100)).orElse(createExpression(employee))
                .map(Mappers.DOUBLE)
                .label(label)
                .orderBy(label)
                .list(getEngine());
        assertEquals(Arrays.asList(1400.0, 2100.0, 2100.0, 3100.0, 3100.0), list);
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).unionAll(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0, 3100.0, 3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).unionAll(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 2100.0, 3100.0, 3100.0, 3100.0, 3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).unionDistinct(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).unionDistinct(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).union(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<Number> list = createExpression(employee).union(createExpression(department.manager())).list(getEngine());
            final List<Double> expected = new ArrayList<>(Arrays.asList(1600.0, 2100.0, 3100.0));
            for (Number n : list) {
                assertTrue(expected + " does not contain " + n, expected.remove(n.doubleValue()));
            }
            assertTrue("Remaining: " + expected, expected.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createExpression(employee).where(employee.lastName.eq("Cooper")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(1600.0, list.get(0).doubleValue());
    }

    public void testQueryValueNegative() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            createExpression(employee).queryValue().pair(department.deptName).list(getEngine());
            fail("Scalar subquery is only allowed to return a single row");
        } catch (SQLException e) {
            // fine
        }
    }

}
