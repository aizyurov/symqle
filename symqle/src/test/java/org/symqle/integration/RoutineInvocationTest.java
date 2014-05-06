package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.CoreMappers;
import org.symqle.common.Mappers;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.One;
import org.symqle.sql.AbstractRoutineInvocation;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Label;
import org.symqle.sql.Params;
import org.symqle.sql.SqlFunction;
import org.symqle.sql.ValueExpression;
import org.symqle.testset.AbstractRoutineInvocationTestSet;
import org.symqle.testset.SqlFunctionTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class RoutineInvocationTest extends AbstractIntegrationTestBase implements AbstractRoutineInvocationTestSet, SqlFunctionTestSet {

    private <T> AbstractRoutineInvocation<T> abs(ValueExpression<T> e) {
        return SqlFunction.create("abs", e.getMapper()).apply(e);
    }

    @Override
    public void test_apply_ValueExpression() throws Exception {
        final Employee employee = new Employee();
        final SqlFunction<Double> abs = SqlFunction.create("abs", Mappers.DOUBLE);
        final List<Double> list = abs.apply(employee.salary.opposite()).add(400.0)
                .map(Mappers.DOUBLE)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(1900.0, 3400.0, 2400.0, 2400.0, 3400.0), list);

    }

    @Override
    public void test_add_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).add(400.0)
                .map(Mappers.DOUBLE)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(1900.0, 3400.0, 2400.0, 2400.0, 3400.0), list);
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.department().manager().salary.add(abs(employee.salary.opposite()))
                .map(Mappers.DOUBLE)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(null, 6000.0, 5000.0, 5000.0, 6000.0), list);
    }

    @Override
    public void test_add_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).add(employee.department().manager().salary)
                .map(Mappers.DOUBLE)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(null, 6000.0, 5000.0, 5000.0, 6000.0), list);
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.in(
                employee.department().manager().salary.asInValueList()
                        .append(abs(employee.salary.sub(3000.0).map(Mappers.DOUBLE)))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.in(
                abs(employee.salary.sub(3000.0).map(Mappers.DOUBLE)).asInValueList()
                        .append(employee.department().manager().salary)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_asBoolean_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("zero"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(-1).also(insertTable.text.set("one"))).execute(getEngine());
            final List<String> list = insertTable.text.where(abs(insertTable.id).asBoolean()).list(getEngine());
            assertEquals(Arrays.asList("one"), list);
        } catch (SQLException e) {
            // ERROR 42846: Cannot convert types 'INTEGER' to 'BOOLEAN'
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .orderBy(abs(employee.salary.opposite()).asc(), employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
    }

    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary).avg().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(2300, list.get(0).intValue());
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).cast("DECIMAL(7,2)").list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_charLength_() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = abs(employee.salary.opposite()).charLength()
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
            final List<String> list = abs(employee.salary.opposite()).map(CoreMappers.STRING).collate(validCollationNameForNumber())
                    .concat(" marsian $")
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("1500 marsian $", "3000 marsian $", "2000 marsian $", "2000 marsian $", "3000 marsian $"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE" at line 1, column 21.
            // org.postgresql.util.PSQLException: ERROR: collations are not supported by type double precision
            //  org.h2.jdbc.JdbcSQLException: Syntax error in SQL statement
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).compileQuery(getEngine()).list();
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = abs(employee.salary.opposite()).concat(employee.lastName)
                    .orderBy(employee.lastName).list(getEngine());
            final List<String> expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = Arrays.asList("1500.0Cooper", "3000.0First", "2000.0March", "2000.0Pedersen", "3000.0Redwood");
            } else {
                expected = Arrays.asList("1500Cooper", "3000First", "2000March", "2000Pedersen", "3000Redwood");
            }
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: Cannot convert types 'DOUBLE' to 'VARCHAR'.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = abs(employee.salary.opposite()).concat(" marsian dollars")
                    .orderBy(employee.lastName).list(getEngine());
            final List<String> expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = Arrays.asList("1500.0 marsian dollars", "3000.0 marsian dollars", "2000.0 marsian dollars", "2000.0 marsian dollars", "3000.0 marsian dollars");
            } else {
                expected = Arrays.asList("1500 marsian dollars", "3000 marsian dollars", "2000 marsian dollars", "2000 marsian dollars", "3000 marsian dollars");
            }
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: Cannot convert types 'DOUBLE' to 'VARCHAR'.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.concat(abs(employee.salary.opposite()))
                    .orderBy(employee.lastName).list(getEngine());
            final List<String> expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = Arrays.asList("Cooper1500.0", "First3000.0", "March2000.0", "Pedersen2000.0", "Redwood3000.0");
            } else {
                expected = Arrays.asList("Cooper1500", "First3000", "March2000", "Pedersen2000", "Redwood3000");
            }
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: Cannot convert types 'DOUBLE' to 'VARCHAR'.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Employee employee = new Employee();
        final One one = new One();
        final List<String> list = employee.lastName.where(abs(one.id).contains(1)).list(getEngine());
        assertEquals(5, list.size());
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = abs(employee.salary).count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = abs(employee.salary).count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .orderBy(abs(employee.salary.opposite()).desc(), employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood", "March", "Pedersen", "Cooper"), list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).distinct().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_div_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).div(employee.department().manager().salary.div(100))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<Integer> percentList = new ArrayList<Integer>();
        for (Number n : list) {
            percentList.add(n == null ? null : n.intValue());
        }
        assertEquals(Arrays.asList(null, 100, 66, 66, 100), percentList);
    }

    @Override
    public void test_div_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).div(0.5)
                .orderBy(employee.lastName).list(getEngine());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(3000.0, 6000.0, 4000.0, 4000.0, 6000.0), asDoubles);
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.department().manager().salary.div(abs(employee.salary.opposite()))
                .map(Mappers.DOUBLE)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(null, 1.0, 1.5, 1.5, 1.0), list);
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).eq(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_eq_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).eq(employee.salary))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.eq(abs(employee.salary.opposite())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).exceptAll(cooper.salary.where(cooper.lastName.eq("Cooper")))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0, 2000.0, 3000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<Double> list = employee.salary.exceptAll(abs(cooper.salary.opposite()).where(cooper.lastName.eq("Cooper")))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0, 2000.0, 3000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).exceptDistinct(cooper.salary.where(cooper.lastName.eq("Cooper")))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<Double> list = employee.salary.exceptDistinct(abs(cooper.salary.opposite()).where(cooper.lastName.eq("Cooper")))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).except(cooper.salary.where(cooper.lastName.eq("Cooper")))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<Double> list = employee.salary.except(abs(cooper.salary.opposite()).where(cooper.lastName.eq("Cooper")))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(2000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> alexDept = department.deptName.where(
                employee.lastName.where(employee.firstName.eq("Alex").and(employee.deptId.eq(department.deptId))).exists())
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), alexDept);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).forReadOnly().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).ge(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(abs(employee.salary.opposite()).ge(employee.department().manager().salary))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.ge(abs(employee.department().manager().salary.opposite())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).gt(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_gt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(abs(employee.salary.sub(4000).opposite().map(Mappers.DOUBLE)).gt(employee.salary))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_gt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.gt(abs(employee.salary.sub(4000).opposite().map(Mappers.DOUBLE))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee hr = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).in(hr.salary.where(hr.department().deptName.eq("HR"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).in(1500.0, 2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.salary.in(abs(department.manager().salary.opposite())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.salary.eq(abs(department.manager().salary.opposite()).all()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_any_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.salary.eq(abs(department.manager().salary.opposite()).any()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.salary.eq(abs(department.manager().salary.opposite()).some()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = abs(employee.salary.opposite()).intersectAll(abs(department.manager().salary.opposite()))
                    .list(getEngine());
            assertEquals(Arrays.asList(3000.0, 3000.0), list);
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
            final List<Double> list = abs(employee.salary.opposite()).intersectAll(abs(department.manager().salary.opposite()))
                    .list(getEngine());
            assertEquals(Arrays.asList(3000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).intersectDistinct(redwood.salary.where(redwood.lastName.eq("Redwood")))
                    .list(getEngine());
            assertEquals(Arrays.asList(3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        try {
            final List<Double> list = employee.salary.intersectDistinct(abs(redwood.salary.opposite()).where(redwood.lastName.eq("Redwood")))
                    .list(getEngine());
            assertEquals(Arrays.asList(3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        try {
            final List<Double> list = abs(employee.salary.opposite()).intersect(redwood.salary.where(redwood.lastName.eq("Redwood")))
                    .list(getEngine());
            assertEquals(Arrays.asList(3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        try {
            final List<Double> list = employee.salary.intersect(abs(redwood.salary.opposite()).where(redwood.lastName.eq("Redwood")))
                    .list(getEngine());
            assertEquals(Arrays.asList(3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.deptId).isNotNull())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.deptId).isNull())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<Double> list = abs(employee.salary.opposite())
                .label(l)
                .orderBy(l)
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).le(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_le_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(abs(employee.salary.opposite()).le(employee.department().manager().salary))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.opposite().le(abs(employee.department().manager().salary.opposite())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(abs(employee.salary).like("20%"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby : No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // rg.postgresql.util.PSQLException: ERROR: operator does not exist: double precision ~~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(abs(employee.salary).like(Params.p("20%")))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby : No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // rg.postgresql.util.PSQLException: ERROR: operator does not exist: double precision ~~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).limit(2, 5).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_countRows_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = abs(employee.salary.opposite()).countRows().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).lt(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_lt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(abs(employee.salary.opposite()).lt(employee.department().manager().salary))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.lt(abs(employee.department().manager().salary.opposite())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = abs(employee.salary.opposite()).map(CoreMappers.INTEGER).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500, 2000, 2000, 3000, 3000), list);
    }

    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary).max().list(getEngine());
        assertEquals(Arrays.asList(3000.0), list);
    }

    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary).min().list(getEngine());
        assertEquals(Arrays.asList(1500.0), list);
    }

    @Override
    public void test_mult_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).mult(employee.department().manager().salary)
                .orderBy(employee.lastName).list(getEngine());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(null, 9000000.0, 6000000.0, 6000000.0, 9000000.0), asDoubles);
    }

    @Override
    public void test_mult_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).mult(2.0).orderBy(employee.lastName).list(getEngine());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(3000.0, 6000.0, 4000.0, 4000.0, 6000.0), asDoubles);
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.mult(abs(employee.department().manager().salary.opposite()))
                .orderBy(employee.lastName).list(getEngine());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(null, 9000000.0, 6000000.0, 6000000.0, 9000000.0), asDoubles);
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).ne(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_ne_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(abs(employee.salary.opposite()).ne(employee.department().manager().salary))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.ne(abs(employee.department().manager().salary.opposite())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee hr = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).notIn(hr.salary.where(hr.department().deptName.eq("HR"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).notIn(1500.0, 2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.salary.notIn(abs(department.manager().salary.opposite())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(abs(employee.salary).notLike("20%"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby : No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // rg.postgresql.util.PSQLException: ERROR: operator does not exist: double precision !~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(abs(employee.salary).notLike(Params.p("20%")))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby : No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // rg.postgresql.util.PSQLException: ERROR: operator does not exist: double precision !~ character varying
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(-1))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.payload.set(-2))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3)).execute(getEngine());
            final List<Integer> list = insertTable.id
                    .orderBy(insertTable.payload.opposite().nullsFirst())
                    .list(getEngine());
            assertEquals(Arrays.asList(3, 1, 2), list);
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
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(-1))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.payload.set(-2))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3)).execute(getEngine());
            final List<Integer> list = insertTable.id
                    .orderBy(abs(insertTable.payload.opposite()).nullsLast())
                    .list(getEngine());
            assertEquals(Arrays.asList(1, 2, 3), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).opposite().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Double> list = employee.salary.gt(1700.0).then(employee.salary.add(100)).orElse(abs(employee.salary.sub(4000)))
                .map(Mappers.DOUBLE)
                .label(label)
                .orderBy(label)
                .list(getEngine());
        assertEquals(Arrays.asList(2100.0, 2100.0, 2500.0, 3100.0, 3100.0), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(abs(employee.salary.opposite()), employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(1500.0, 3000.0, 2000.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double,String>> list = abs(employee.salary.opposite()).pair(employee.lastName)
                .where(employee.deptId.isNull())
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1500.0, "Cooper")), list);
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Double>> list = employee.lastName.pair(abs(employee.salary.opposite()))
                .where(employee.deptId.isNull())
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make("Cooper", 1500.0)), list);
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final AbstractRoutineInvocation<Double> abs = abs(employee.salary.opposite());
        final DynamicParameter<Double> param = abs.param();
        param.setValue(1500.0);
        final List<String> list = employee.lastName.where(abs.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final AbstractRoutineInvocation<Double> abs = abs(employee.salary.opposite());
        final DynamicParameter<Double> param = abs.param(1500.0);
        final List<String> list = employee.lastName.where(abs.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_positionOf_String() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = abs(employee.salary.opposite()).positionOf("500")
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
            final List<Integer> list = abs(employee.salary.opposite()).positionOf(employee.lastName)
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
            final AbstractRoutineInvocation<Double> abs = abs(employee.salary.opposite());
            final List<Integer> list = employee.lastName.concat(abs).positionOf(abs)
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
        final Department department = new Department();
        final One one = new One();
        final List<Pair<String, Integer>> list = department.deptName.pair(abs(one.id.opposite()).queryValue())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make("DEV", 1), Pair.make("HR", 1)), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> expected = new ArrayList<>(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0));
        final int count = abs(employee.salary.opposite()).scroll(getEngine(), new Callback<Double>() {
            @Override
            public boolean iterate(final Double aDouble) throws SQLException {
                assertTrue(expected + "does not contain " + aDouble, expected.remove(aDouble));
                return true;
            }
        });
        assertEquals(5, count);
    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).selectAll().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("zero").also(insertTable.payload.set(-3)))).execute(getEngine());
        final List<Integer> list = insertTable.payload.list(getEngine());
        assertEquals(Arrays.asList(-3), list);
        insertTable.update(insertTable.payload.set(abs(insertTable.payload))).execute(getEngine());
        final List<Integer> updated = insertTable.payload.list(getEngine());
        assertEquals(Arrays.asList(3), updated);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = abs(employee.salary.opposite()).showQuery(getEngine().getDialect());
        assertSimilar("SELECT abs(- T0.salary) AS C0 FROM employee AS T0", sql);
    }

    @Override
    public void test_sub_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).sub(100.0)
                .orderBy(employee.lastName).list(getEngine());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(1400.0, 2900.0, 1900.0, 1900.0, 2900.0), asDoubles);
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.sub(abs(employee.department().manager().salary.opposite()))
                .orderBy(employee.lastName).list(getEngine());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(null, 0.0, -1000.0, -1000.0, 0.0), asDoubles);
    }

    @Override
    public void test_sub_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary.opposite()).sub(employee.department().manager().salary)
                .orderBy(employee.lastName).list(getEngine());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(null, 0.0, -1000.0, -1000.0, 0.0), asDoubles);
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = abs(employee.salary.opposite())
                    .substring(employee.lastName.charLength().div(3)).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            final String expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = "500.0";
            } else {
                expected = "500";
            }
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
            final List<String> list = abs(employee.salary.opposite())
                    .substring(employee.lastName.charLength().div(3), employee.lastName.charLength().div(3))
                    .where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList("50"), list);
        } catch (SQLException e) {
            // ERROR 42X25: The 'SUBSTR' function is not allowed on the 'DOUBLE' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(double precision, numeric) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(-2)
                        .also(insertTable.text.set("abcdef")))).execute(getEngine());
        final List<String> list = insertTable.text.substring(abs(insertTable.payload)).list(getEngine());
        assertEquals(Arrays.asList("bcdef"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(-2)
                        .also(insertTable.text.set("abcdef")))).execute(getEngine());
        final List<String> list = insertTable.text
                .substring(abs(insertTable.payload), abs(insertTable.payload))
                .list(getEngine());
        assertEquals(Arrays.asList("bc"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(-2)
                        .also(insertTable.text.set("abcdef")))).execute(getEngine());
        final List<String> list = insertTable.text
                .substring(abs(insertTable.payload), abs(insertTable.payload))
                .list(getEngine());
        assertEquals(Arrays.asList("bc"), list);
    }

    @Override
    public void test_substring_int() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = abs(employee.salary.opposite()).substring(2).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            final String expected;
            if (SupportedDb.H2.equals(getDatabaseName())) {
                expected = "500.0";
            } else {
                expected = "500";
            }
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
            final List<String> list = abs(employee.salary.opposite()).substring(2, 2).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList("50"), list);
        } catch (SQLException e) {
            // ERROR 42X25: The 'SUBSTR' function is not allowed on the 'DOUBLE' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(double precision, integer) does not exist
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = abs(employee.salary).sum().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(11500, list.get(0).intValue());
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<Double> list = employee.salary.lt(1700.0).then(abs(employee.salary.sub(4000))).orElse(employee.salary.add(100))
                .map(Mappers.DOUBLE)
                .label(label)
                .orderBy(label)
                .list(getEngine());
        assertEquals(Arrays.asList(2100.0, 2100.0, 2500.0, 3100.0, 3100.0), list);
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = employee.salary.unionAll(abs(department.manager().salary.opposite()))
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).unionAll(cooper.salary.where(cooper.lastName.eq("Cooper")))
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = employee.salary.unionDistinct(abs(department.manager().salary.opposite()))
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).unionDistinct(cooper.salary.where(cooper.lastName.eq("Cooper")))
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = employee.salary.union(abs(department.manager().salary.opposite()))
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).union(cooper.salary.where(cooper.lastName.eq("Cooper")))
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).where(employee.lastName.eq("Cooper")).list(getEngine());
        assertEquals(Arrays.asList(1500.0), list);
    }

    public void testAsFunctionArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(abs(employee.salary.opposite())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

}
