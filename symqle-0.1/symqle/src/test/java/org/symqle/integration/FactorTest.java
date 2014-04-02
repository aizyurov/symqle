package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.CoreMappers;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.integration.model.One;
import org.symqle.sql.AbstractFactor;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Label;
import org.symqle.sql.Mappers;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractFactorTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class FactorTest extends AbstractIntegrationTestBase implements AbstractFactorTestSet {


    private AbstractFactor<Double> createFactor(final Employee employee) {
        return employee.salary.opposite();
    }

    @Override
    public void test_add_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).add(3000.0).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(0.0, 0.0, 1000.0, 1000.0, 1500.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.mult(2).add(createFactor(employee)).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_add_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).add(employee.salary.mult(2)).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.sub(6000).map(Mappers.DOUBLE)
                .in(Params.p(-4500.0).asInValueList().append(createFactor(employee))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.sub(6000).map(Mappers.DOUBLE)
                .in(createFactor(employee).asInValueList().append(Params.p(-4500.0))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_asPredicate_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("zero"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(-1).also(insertTable.text.set("one"))).execute(getEngine());
            final List<String> list = insertTable.text.where(insertTable.id.opposite().asPredicate()).list(getEngine());
            assertEquals(Arrays.asList("one"), list);
        } catch (SQLException e) {
            // ERROR 42846: Cannot convert types 'INTEGER' to 'BOOLEAN'
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).orderBy(createFactor(employee).asc()).list(getEngine());
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).avg().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(-2300.0, list.get(0).doubleValue());
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).cast("DECIMAL(7,2)").list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_charLength_() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = createFactor(employee).charLength()
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(1, list.size());
            int value = list.get(0);
            // different databases return different values
            assertTrue("Not expected: " + value, value > 4 && value < 10);
            final String stringRep = createFactor(employee).map(Mappers.STRING)
                    .where(employee.lastName.eq("Redwood")).list(getEngine()).get(0);
            System.out.println("Value as string: ["+stringRep+"]");
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function char_length(double precision) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createFactor(employee).map(CoreMappers.STRING).collate(validCollationNameForNumber())
                    .concat(" marsian $")
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("-1500 marsian $", "-3000 marsian $", "-2000 marsian $", "-2000 marsian $", "-3000 marsian $"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE" at line 1, column 21.
            // org.postgresql.util.PSQLException: ERROR: collations are not supported by type double precision
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).compileQuery(getEngine()).list();
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createFactor(employee).concat(employee.lastName)
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("-1500Cooper", "-3000First", "-2000March", "-2000Pedersen", "-3000Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createFactor(employee).concat(" marsian $")
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("-1500 marsian $", "-3000 marsian $", "-2000 marsian $", "-2000 marsian $", "-3000 marsian $"), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.concat(createFactor(employee))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper-1500", "First-3000", "March-2000", "Pedersen-2000", "Redwood-3000"), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createFactor(employee).contains(-3000.0))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createFactor(employee).countDistinct().list(getEngine());
        assertEquals(Arrays.asList(3), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createFactor(employee).count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).orderBy(createFactor(employee).desc()).list(getEngine());
        assertEquals(Arrays.asList(-1500.0, -2000.0, -2000.0, -3000.0, -3000.0), list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).distinct().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_div_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).div(createFactor(employee).div(employee.salary)).list(getEngine());
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
        final List<Number> list = createFactor(employee).div(0.5).list(getEngine());
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
        final List<Number> list = employee.salary.div(createFactor(employee))
                .where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(-1.0, list.get(0).doubleValue());
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).eq(-1500.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_eq_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).eq(employee.salary.sub(6000.0).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.sub(6000.0).map(Mappers.DOUBLE).eq(createFactor(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = department.manager().salary.opposite().exceptAll(createFactor(employee)).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = createFactor(employee).exceptAll(department.manager().salary.opposite()).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(-2000.0, -2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = department.manager().salary.opposite().exceptDistinct(createFactor(employee)).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = createFactor(employee).exceptDistinct(department.manager().salary.opposite()).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(-2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = department.manager().salary.opposite().exceptDistinct(createFactor(employee)).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = createFactor(employee).except(department.manager().salary.opposite()).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(-2000.0, -1500.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createFactor(employee).exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).forReadOnly().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).ge(-2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_ge_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).ge(employee.salary.sub(4000).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_ge_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.sub(4000).map(Mappers.DOUBLE).ge(createFactor(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).gt(-2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_gt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).gt(employee.salary.sub(4000).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_gt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.sub(4000).map(Mappers.DOUBLE).gt(createFactor(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createFactor(employee).in(department.manager().salary.opposite()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).in(-2000.0, -1500.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createFactor(employee).in(department.manager().salary.opposite()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = createFactor(employee).intersectAll(department.manager().salary.opposite()).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = createFactor(employee).intersectAll(department.manager().salary.opposite()).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0, -3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = createFactor(employee).intersectDistinct(department.manager().salary.opposite()).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = createFactor(employee).intersectDistinct(department.manager().salary.opposite()).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = createFactor(employee).intersect(department.manager().salary.opposite()).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<Double> list = createFactor(employee).intersect(department.manager().salary.opposite()).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(-3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractFactor<Integer> factor = employee.deptId.opposite();
        final List<String> list = employee.lastName.where(factor.isNotNull()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractFactor<Integer> factor = employee.deptId.opposite();
        final List<String> list = employee.lastName.where(factor.isNull()).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<Double> list = createFactor(employee).label(l).where(employee.firstName.eq("James"))
                .orderBy(l).list(getEngine());
        assertEquals(Arrays.asList(-3000.0, -1500.0), list);
    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).le(-2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).le(employee.salary.sub(4000).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.sub(4000).map(Mappers.DOUBLE).le(createFactor(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createFactor(employee).like("-2%"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: double precision ~~ character varying
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createFactor(employee).like(Params.p("-2%")))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: double precision ~~ character varying
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).limit(2, 5).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).lt(-2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_lt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).lt(employee.salary.sub(4000).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.sub(4000).map(Mappers.DOUBLE).lt(createFactor(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createFactor(employee).map(CoreMappers.INTEGER).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000, -3000, -2000, -2000, -1500), list);
    }

    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).max().list(getEngine());
        assertEquals(Arrays.asList(-1500.0), list);
    }

    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).min().list(getEngine());
        assertEquals(Arrays.asList(-3000.0), list);
    }

    @Override
    public void test_mult_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).mult(employee.salary.div(createFactor(employee))).list(getEngine());
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
        final List<Number> list = createFactor(employee).mult(2).list(getEngine());
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
        final List<Number> list = employee.salary.div(createFactor(employee)).mult(createFactor(employee)).list(getEngine());
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
                .where(createFactor(employee).ne(-1500.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ne_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).ne(employee.salary.sub(6000.0).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.salary.sub(6000.0).map(Mappers.DOUBLE).ne(createFactor(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createFactor(employee).notIn(department.manager().salary.opposite()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createFactor(employee).notIn(-2000.0, -1500.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(createFactor(employee).notIn(department.manager().salary.opposite()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createFactor(employee).notLike("-2%"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: double precision !~ character varying
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createFactor(employee).notLike(Params.p("-2%")))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42884: No authorized routine named 'LIKE' of type 'FUNCTION' having compatible arguments was found.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: double precision !~ character varying
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(1))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.payload.set(2))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3)).execute(getEngine());
            final List<Integer> list = insertTable.id
                    .orderBy(insertTable.payload.opposite().nullsFirst())
                    .list(getEngine());
            assertEquals(Arrays.asList(3, 2, 1), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(1))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.payload.set(2))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3)).execute(getEngine());
            final List<Integer> list = insertTable.id
                    .orderBy(insertTable.payload.opposite().nullsLast())
                    .list(getEngine());
            assertEquals(Arrays.asList(2, 1, 3), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).opposite().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.retired.asPredicate().negate().then(employee.salary).orElse(createFactor(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).orderBy(createFactor(employee)).list(getEngine());
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).orderBy(createFactor(employee)).list(getEngine());
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = createFactor(employee).pair(employee.lastName).where(employee.retired.asPredicate()).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(-1500.0, "Cooper")), list);
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Double>> list = employee.lastName.pair(createFactor(employee)).where(employee.retired.asPredicate()).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("Cooper", -1500.0)), list);
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final AbstractFactor<Double> factor = createFactor(employee);
        final DynamicParameter<Double> param = factor.param();
        param.setValue(-1500.0);
        final List<String> list = employee.lastName.where(factor.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final AbstractFactor<Double> factor = createFactor(employee);
        final DynamicParameter<Double> param = factor.param(-1500.0);
        final List<String> list = employee.lastName.where(factor.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_positionOf_String() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = createFactor(employee).positionOf("500")
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            assertEquals(Arrays.asList(3), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.position(double precision, character varying) does not exist
            // apache Derby: ava.sql.SQLException: Java exception: ': java.lang.NullPointerException'.
            expectSQLException(e, "PostgreSQL", "Apache Derby");
        }
    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = createFactor(employee).positionOf(employee.lastName)
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            assertEquals(Arrays.asList(0), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.position(double precision, character varying) does not exist
            // apache Derby: ava.sql.SQLException: Java exception: ': java.lang.NullPointerException'.
            expectSQLException(e, "PostgreSQL", "Apache Derby");
        }
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = employee.lastName.concat(createFactor(employee)).positionOf(createFactor(employee))
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            assertEquals(Arrays.asList(7), list);
        } catch (SQLException e) {
            // Apache Derby: ERROR 42846: Cannot convert types 'DOUBLE' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.position(text, double precision) does not exist
            expectSQLException(e, "PostgreSQL", "Apache Derby");
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
        final int callCount = createFactor(employee).scroll(getEngine(), new Callback<Double>() {
            @Override
            public boolean iterate(final Double aDouble) throws SQLException {
                assertTrue(expected.toString() + "does not contain " + aDouble, expected.remove(aDouble));
                return true;
            }
        });
        assertEquals(5, callCount);
    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).selectAll().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("zero").also(insertTable.payload.set(3)))).execute(getEngine());
        final List<Integer> list = insertTable.payload.list(getEngine());
        assertEquals(Arrays.asList(3), list);
        insertTable.update(insertTable.payload.set(insertTable.payload.opposite())).execute(getEngine());
        final List<Integer> updated = insertTable.payload.list(getEngine());
        assertEquals(Arrays.asList(-3), updated);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = createFactor(employee).showQuery(getEngine().getDialect());
        Pattern expected = Pattern.compile("SELECT - ([A-Z][A-Z0-9]*).salary AS [A-Z][A-Z0-9]* FROM employee AS \\1");
        assertTrue(sql, expected.matcher(sql).matches());
    }

    @Override
    public void test_sub_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).sub(500.0).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(-3500.0, -3500.0, -2500.0, -2500.0, -2000.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.sub(createFactor(employee)).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(3000.0, 4000.0, 4000.0, 6000.0, 6000.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_sub_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).sub(employee.salary).list(getEngine());
        final List<Double> actual = new ArrayList<Double>();
        for (Number number : list) {
            actual.add(number.doubleValue());
        }
        Collections.sort(actual);
        final List<Double> expected = Arrays.asList(-6000.0, -6000.0, -4000.0, -4000.0, -3000.0);
        assertEquals(expected, actual);
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = createFactor(employee).substring(employee.lastName.charLength().div(3)).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList("1500"), list);
        } catch (SQLException e) {
            // ERROR 42X25: The 'SUBSTR' function is not allowed on the 'DOUBLE' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(double precision, numeric) does not exist
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = createFactor(employee)
                    .substring(employee.lastName.charLength().div(3), employee.lastName.charLength().div(3))
                    .where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList("15"), list);
        } catch (SQLException e) {
            // ERROR 42X25: The 'SUBSTR' function is not allowed on the 'DOUBLE' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(double precision, numeric) does not exist
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(-2)
                        .also(insertTable.text.set("abcdef")))).execute(getEngine());
        final List<String> list = insertTable.text.substring(insertTable.payload.opposite()).list(getEngine());
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
                .substring(insertTable.payload.opposite(), insertTable.payload.opposite())
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
                .substring(insertTable.payload.opposite(), insertTable.payload.opposite())
                .list(getEngine());
        assertEquals(Arrays.asList("bc"), list);
    }

    @Override
    public void test_substring_int() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = createFactor(employee).substring(2).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList("1500"), list);
        } catch (SQLException e) {
            // ERROR 42X25: The 'SUBSTR' function is not allowed on the 'DOUBLE' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(double precision, integer) does not exist
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_substring_int_int() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = createFactor(employee).substring(2, 2).where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList("15"), list);
        } catch (SQLException e) {
            // ERROR 42X25: The 'SUBSTR' function is not allowed on the 'DOUBLE' type.
            // org.postgresql.util.PSQLException: ERROR: function pg_catalog.substring(double precision, integer) does not exist
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = createFactor(employee).sum().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(-11500.0, list.get(0).doubleValue());
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.retired.asPredicate().then(createFactor(employee)).orElse(employee.salary).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = createFactor(employee).unionAll(department.manager().salary.opposite()).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = createFactor(employee).unionAll(department.manager().salary.opposite()).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = createFactor(employee).unionDistinct(department.manager().salary.opposite()).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = createFactor(employee).unionDistinct(department.manager().salary.opposite()).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = createFactor(employee).union(department.manager().salary.opposite()).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Double> list = createFactor(employee).union(department.manager().salary.opposite()).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -2000.0, -1500.0), list);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createFactor(employee).where(employee.lastName.eq("Cooper")).list(getEngine());
        assertEquals(Arrays.asList(-1500.0), list);
    }

    public void testQueryValueNegative() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            createFactor(employee).queryValue().pair(department.deptName).list(getEngine());
            fail("Scalar subquery is only allowed to return a single row");
        } catch (SQLException e) {
            // fine
        }
    }

}
