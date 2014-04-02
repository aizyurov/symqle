package org.symqle.integration;

import junit.framework.AssertionFailedError;
import org.symqle.common.Callback;
import org.symqle.common.CompiledSql;
import org.symqle.common.CoreMappers;
import org.symqle.common.Pair;
import org.symqle.common.SqlParameters;
import org.symqle.integration.model.BigTable;
import org.symqle.integration.model.Country;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.jdbc.Batcher;
import org.symqle.jdbc.Engine;
import org.symqle.jdbc.Option;
import org.symqle.querybuilder.StringSql;
import org.symqle.sql.AbstractQuerySpecificationScalar;
import org.symqle.sql.AbstractSelectList;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Functions;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Label;
import org.symqle.sql.Mappers;
import org.symqle.testset.ColumnTestSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class ColumnTest extends AbstractIntegrationTestBase implements ColumnTestSet {

    private final static List<Option> NO_OPTIONS = Collections.<Option>emptyList();

    @Override
    public void onSetUp() throws Exception {
        final Country country = new Country();
        final Department dept = new Department();
    }

    public void testDialect() throws Exception {
        assertEquals(getEngine().getDatabaseName(), getEngine().getDialect().getName());
    }

    @Override
    public void test_add_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Number> sums = employee.empId.add(1).list(getEngine());
        final List<Integer> list = employee.empId.list(getEngine());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n : sums) {
            actual.add(n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Integer i : list) {
            expected.add(i + 1);
        }
        assertEquals(expected, actual);
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.empId.add(employee.deptId).list(getEngine());
        final List<Pair<Integer, Integer>> pairs = employee.empId.pair(employee.deptId).list(getEngine());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : pairs) {
            expected.add(pair.first() == null || pair.second() == null ? null : pair.first() + pair.second());
        }
        assertEquals(expected, actual);
    }

    @Override
    public void test_add_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.empId.add(employee.deptId).list(getEngine());
        final List<Pair<Integer, Integer>> pairs = employee.empId.pair(employee.deptId).list(getEngine());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : pairs) {
            expected.add(pair.first() == null || pair.second() == null ? null : pair.first() + pair.second());
        }
        assertEquals(expected, actual);
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.empId.in(
                employee.empId.asInValueList().append(employee.department().manager().empId)))
                .list(getEngine());
        assertEquals(5, list.size());
    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.empId.in(
                employee.department().manager().empId.asInValueList()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_asPredicate_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.retired.asPredicate()).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName.asc()).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        assertEquals(expected, list);
    }

    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.avg().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(2300, list.get(0).intValue());
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.cast("CHAR(8)").list(getEngine());
        Collections.sort(list);
        try {
            assertEquals(Arrays.asList("Cooper  ", "First   ", "March   ", "Pedersen", "Redwood "), list);
        } catch (AssertionFailedError e) {
            if ("MySQL".equals(getDatabaseName())) {
                // mysql trheats CHAR as VARCHAR, blanks are not appended
                assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void test_charLength_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.charLength().where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final AbstractQuerySpecificationScalar<String> querySpecificationScalar = employee.firstName.collate(validCollationNameForVarchar()).where(employee.lastName.eq("Redwood"));
            final List<String> list = querySpecificationScalar.list(getEngine());
            assertEquals(Arrays.asList("Margaret"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE" at line 1, column 63
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.compileQuery(getEngine()).list();
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));

    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.concat(employee.lastName).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("MargaretRedwood"), list);
    }

    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.concat(" expected").where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("Margaret expected"), list);
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.concat(employee.lastName).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("MargaretRedwood"), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Country country = new Country();
        final Department department = new Department();
        final List<String> list = country.code.where(department.deptId.contains(-1)).list(getEngine());
        assertEquals(0, list.size());
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.firstName.countDistinct().list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName.desc()).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        Collections.reverse(expected);
        assertEquals(expected, list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.distinct().list(getEngine());
        assertEquals(4, list.size());
        assertTrue(list.toString(), list.contains("Margaret"));
        assertTrue(list.toString(), list.contains("Bill"));
        assertTrue(list.toString(), list.contains("James"));
        assertTrue(list.toString(), list.contains("Alex"));
    }

    @Override
    public void test_div_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.div(employee.salary.sub(2000.0))
                .map(Mappers.DOUBLE)
                .where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(Arrays.asList(3.0), list);
    }

    @Override
    public void test_div_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.div(1000.0)
                .map(Mappers.DOUBLE)
                .where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(Arrays.asList(3.0), list);
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.add(3000.0).div(employee.salary)
                .map(Mappers.DOUBLE)
                .where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(Arrays.asList(2.0), list);
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James")).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Cooper")), new HashSet<String>(list));
    }

    @Override
    public void test_eq_Predicand() throws Exception {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName
                    .where(employee.firstName.eq(employee.department().manager().firstName))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.department().manager().firstName.eq(employee.firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.exceptAll(department.manager().lastName).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Cooper")), new HashSet<String>(list));
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
            final List<String> list = employee.lastName.exceptAll(department.manager().lastName).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Cooper")), new HashSet<String>(list));
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
            final List<String> list = employee.firstName.exceptDistinct(department.manager().firstName.where(department.deptId.eq(1))).list(getEngine());
            assertEquals(3, list.size());
            assertTrue(list.toString(), list.contains("Bill"));
            assertTrue(list.toString(), list.contains("James"));
            assertTrue(list.toString(), list.contains("Alex"));
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
            final List<String> list = employee.firstName.exceptDistinct(department.manager().firstName.where(department.deptId.eq(1))).list(getEngine());
            assertEquals(3, list.size());
            assertTrue(list.toString(), list.contains("Bill"));
            assertTrue(list.toString(), list.contains("James"));
            assertTrue(list.toString(), list.contains("Alex"));
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
            final List<String> list = employee.lastName.except(department.manager().lastName).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Cooper")), new HashSet<String>(list));
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
            final List<String> list = employee.lastName.except(department.manager().lastName).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Cooper")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Country country = new Country();
        final Department department = new Department();
        final List<String> list = country.code.where(department.deptId.exists())
                .orderBy(country.code)
                .list(getEngine());
        assertEquals(Arrays.asList("FRA", "RUS", "USA"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.forReadOnly().list(getEngine());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.forUpdate().list(getEngine());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ge("James")).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Cooper", "Redwood")), new HashSet<String>(list));
    }

    @Override
    public void test_ge_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ge(employee.department().manager().firstName)).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
    }

    @Override
    public void test_ge_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ge(employee.department().manager().firstName)).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.gt("James")).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
    }

    @Override
    public void test_gt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.gt(employee.department().manager().firstName)).list(getEngine());
        assertEquals(Collections.emptyList(), list);
    }

    @Override
    public void test_gt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.gt(employee.department().manager().firstName)).list(getEngine());
        assertEquals(Collections.emptyList(), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.in(department.manager().firstName)).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.in("James", "Bill")).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Cooper"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.in(department.manager().firstName)).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersectAll(department.manager().lastName).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersectAll(department.manager().lastName).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersectDistinct(department.manager().lastName).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersectDistinct(department.manager().lastName).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersect(department.manager().lastName).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersect(department.manager().lastName).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.deptId.isNotNull())
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("First", "March", "Pedersen", "Redwood");
        assertEquals(expected, list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.deptId.isNull()).list(getEngine());
        assertEquals(Collections.singletonList("Cooper"), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        Label l = new Label();
        final List<String> list = employee.lastName.label(l).where(employee.firstName.notLike("%es")).orderBy(l).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "Redwood"));
        assertEquals(expected, list);
    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.le("James")).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "First", "Cooper")), new HashSet<String>(list));
    }

    @Override
    public void test_le_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.le(employee.department().manager().firstName)).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "First", "Redwood")), new HashSet<String>(list));
    }

    @Override
    public void test_le_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.le(employee.department().manager().firstName)).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "First", "Redwood")), new HashSet<String>(list));
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.like("%es"))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("Cooper", "First");
        assertEquals(expected, list);
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.like(employee.firstName.substring(1,4).concat("_")))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("Cooper", "First");
        assertEquals(expected, list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.limit(4).list(getEngine());
        assertEquals(4, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.limit(2, 3).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.list(getEngine());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.lt("James")).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    @Override
    public void test_lt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.lt(employee.department().manager().firstName)).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    @Override
    public void test_lt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.lt(employee.department().manager().firstName)).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.salary.map(CoreMappers.INTEGER).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(1500, 3000, 2000, 2000, 3000), list);
    }

    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.max().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(3000, list.get(0).intValue());
    }

    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.min().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(1500, list.get(0).intValue());
    }

    @Override
    public void test_mult_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.mult(employee.salary.div(1000))
                .where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(9000, list.get(0).intValue());
    }

    @Override
    public void test_mult_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.mult(3)
                .where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(9000, list.get(0).intValue());
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.div(1000).mult(employee.salary)
                .where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(9000, list.get(0).intValue());
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ne("James")).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood")), new HashSet<String>(list));
    }

    @Override
    public void test_ne_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ne(employee.department().manager().firstName)).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    @Override
    public void test_ne_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ne(employee.department().manager().firstName)).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.firstName.notIn(department.manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("March", "Pedersen");
        assertEquals(expected, list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.notIn("James", "Bill"))
                .orderBy(employee.lastName.desc())
                .list(getEngine());
        final List<String> expected = Arrays.asList("Redwood", "Pedersen");
        assertEquals(expected, list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.firstName.notIn(department.manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("March", "Pedersen");
        assertEquals(expected, list);
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.notLike("%es"))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("March", "Pedersen", "Redwood");
        assertEquals(expected, list);
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.notLike(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("March", "Pedersen");
        assertEquals(expected, list);
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(employee.deptId.nullsFirst()).list(getEngine());
            assertEquals("Cooper", list.get(0));
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(employee.deptId.nullsLast()).list(getEngine());
            assertEquals("Cooper", list.get(list.size()-1));
        } catch (SQLException e) {
            // mysql does not support NULLS LAST
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.opposite().where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList(-3000.0), list);
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.salary.gt(2000.0).then(employee.lastName).orElse(employee.firstName).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("James", "Cooper"),
                Pair.make("First", "First"),
                Pair.make("Bill", "March"),
                Pair.make("Alex", "Pedersen"),
                Pair.make("Redwood", "Redwood")
        ), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James"))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First"), list);

    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James"))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("James", "Cooper"),
                Pair.make("James", "First"),
                Pair.make("Bill", "March"),
                Pair.make("Alex", "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("James", "Cooper"),
                Pair.make("James", "First"),
                Pair.make("Bill", "March"),
                Pair.make("Alex", "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final DynamicParameter<String> param = employee.lastName.param();
        param.setValue(" ==");
        final List<String> list = employee.lastName.concat(param).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(Arrays.asList("First =="), list);
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final DynamicParameter<String> param = employee.lastName.param(" ==");
        final List<String> list = employee.lastName.concat(param).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(Arrays.asList("First =="), list);
    }

    @Override
    public void test_positionOf_String() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.firstName.positionOf("gar").where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = employee.firstName.positionOf(employee.firstName.substring(4, 3))
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList(4), list);
        } catch (SQLException e) {
            // java.sql.SQLException: The exception 'java.lang.NoSuchMethodError: org.apache.derby.iapi.types.ConcatableDataValue.locate(Lorg/apache/derby/iapi/types/StringDataValue;Lorg/apache/derby/iapi/types/NumberDataValue;Lorg/apache/derby/iapi/types/NumberDataValue;)Lorg/apache/derby/iapi/types/NumberDataValue;' was thrown while evaluating an expression.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.concat(employee.firstName).positionOf(employee.firstName)
                .where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList(8), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        MyDual dual = new MyDual();
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = dual.dummy.queryValue().pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("X", "Cooper"), Pair.make("X", "First"), Pair.make("X", "March"), Pair.make("X", "Pedersen"), Pair.make("X", "Redwood")), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final Set<String> names = new HashSet<>(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"));
        final int iterations = employee.lastName.scroll(getEngine(), new Callback<String>() {
            @Override
            public boolean iterate(final String s) throws SQLException {
                assertTrue(names.toString(), names.remove(s));
                return true;
            }
        });
        assertEquals(5, iterations);
    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.selectAll().list(getEngine());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    @Override
    public void test_setDefault_() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        insertTable.update(insertTable.text.setDefault()).execute(getEngine());
        final List<String> list = insertTable.text.list(getEngine());
        assertEquals(Arrays.asList("nothing"), list);
    }

    @Override
    public void test_setNull_() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        insertTable.update(insertTable.text.setNull()).execute(getEngine());
        final List<String> list = insertTable.text.list(getEngine());
        assertEquals(1, list.size());
        assertNull(list.get(0));
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        insertTable.update(insertTable.text.set(insertTable.text)).execute(getEngine());
        final List<String> list = insertTable.text.list(getEngine());
        assertEquals(Arrays.asList("one"), list);
    }

    @Override
    public void test_set_Object() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        insertTable.update(insertTable.text.set("ein")).execute(getEngine());
        final List<String> list = insertTable.text.list(getEngine());
        assertEquals(Arrays.asList("ein"), list);
    }

    @Override
    public void test_set_ValueExpression() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        insertTable.update(insertTable.text.set(insertTable.text.concat("=="))).execute(getEngine());
        final List<String> list = insertTable.text.list(getEngine());
        assertEquals(Arrays.asList("one=="), list);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = employee.lastName.showQuery(new GenericDialect());
        Pattern pattern = Pattern.compile("SELECT ([A-Z][A-Z0-9]+).last_name AS [A-Z][A-Z0-9]+ FROM employee AS \\1");
        assertTrue(sql, pattern.matcher(sql).matches());
    }

    @Override
    public void test_sub_Number() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.sub(1000.0).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(2000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.add(1000.0).sub(employee.salary).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(1000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_sub_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.sub(employee.salary.div(3)).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(2000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.firstName.substring(employee.firstName.charLength().div(2))
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList("garet"), list);
        } catch (SQLException e) {
            // ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.firstName.substring(employee.firstName.charLength().div(2), employee.firstName.charLength().div(2))
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList("gare"), list);
        } catch (SQLException e) {
            // ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.substring(employee.empId)
                .where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("Margaret"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.firstName.substring(employee.empId, employee.firstName.charLength().div(2))
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList("Marg"), list);
        } catch (SQLException e) {
            // ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.firstName.substring(employee.firstName.charLength().div(2), employee.empId)
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList("g"), list);
        } catch (SQLException e) {
            // ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_substring_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.substring(4)
                .where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("garet"), list);
    }

    @Override
    public void test_substring_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.substring(4, 3)
                .where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("gar"), list);
    }

    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.sum().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(11500, list.get(0).intValue());
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.salary.gt(2000.0).then(employee.lastName).orElse(employee.firstName).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("James", "Cooper"),
                Pair.make("First", "First"),
                Pair.make("Bill", "March"),
                Pair.make("Alex", "Pedersen"),
                Pair.make("Redwood", "Redwood")
        ), list);
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.unionAll(department.manager().lastName).list(getEngine());
        assertEquals(7, list.size());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood", "First", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.unionAll(department.manager().lastName).list(getEngine());
        assertEquals(7, list.size());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood", "First", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.unionDistinct(department.manager().lastName).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.unionDistinct(department.manager().lastName).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.union(department.manager().lastName).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.union(department.manager().lastName).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("Margaret"), list);
    }

    public void testSubqueryInSelect() throws Exception {
        final Employee employee = new Employee();
        final Employee manager = new Employee();
        final List<Pair<String, String>> pairs = employee.lastName
                .pair(
                        manager.lastName.where(employee.department().deptId.eq(manager.department().deptId)
                                .and(manager.title.like("%manager"))).queryValue()
                )
                .where(employee.title.eq("guru")).list(getEngine());
        assertEquals(1, pairs.size());
        assertEquals(Pair.make("Pedersen", "First"), pairs.get(0));
    }

    public void testMultipleJoins() throws Exception {
        System.out.println(getName());
        final Employee employee = new Employee();
        final AbstractSelectList<Pair<String,String>> select = employee.department().country().code.pair(employee.department().manager().department().country().code);
        final List<Pair<String, String>> countryCodes = select.list(getEngine());
        assertEquals(5, countryCodes.size());
        System.out.println(countryCodes);
        for (Pair<String, String> pair : countryCodes) {
            assertEquals(pair.first(), pair.second());
        }
    }

    public void testAsFunctionArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = Functions.floor(employee.salary).list(getEngine());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(3000.0));
        assertTrue(list.toString(), list.contains(2000.0));
        assertTrue(list.toString(), list.contains(1500.0));
    }

    public void testExistsWithCondition() throws Exception {
        final Country country = new Country();
        final Department department = new Department();
        final List<String> list = country.code.where(department.deptId.where(department.countryId.eq(country.countryId)).exists()).list(getEngine());
        assertEquals(new HashSet<String>(Arrays.asList("RUS", "USA")), new HashSet<String>(list));
    }

    public void testOrderByTwoColumns() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.firstName, employee.lastName).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Pedersen", "March", "Cooper", "First", "Redwood"));
        assertEquals(expected, list);
    }

    public void testLabelOrderByDesc() throws Exception {
        final Employee employee = new Employee();
        Label l = new Label();
        final List<String> list = employee.lastName.label(l).where(employee.firstName.notLike("%es")).orderBy(l.desc()).list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "Pedersen", "March"));
        assertEquals(expected, list);
    }

    public void testMaxRows() throws Exception {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.orderBy(employee.lastName).list(getEngine(), Option.setMaxRows(2));
            assertEquals(2, list.size());
            assertTrue(list.toString(), list.contains("First"));
            assertTrue(list.toString(), list.contains("Cooper"));
    }

    public void testFetchSize() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.list(getEngine(), Option.setFetchSize(2));
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    public void testFetchDirection() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.orderBy(employee.lastName).list(getEngine(), Option.setFetchDirection(ResultSet.FETCH_REVERSE));
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: Operation requires a scrollable ResultSet, but this ResultSet is FORWARD_ONLY
            expectSQLException(e, "PostgreSQL");
        }
    }

    public void testMaxFieldSize() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName).list(getEngine(), Option.setMaxFieldSize(5));
        final ArrayList<String> truncatedNames = new ArrayList<String>(Arrays.asList("March", "First", "Peder", "Redwo", "Coope"));
        final ArrayList<String> fullNames = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(truncatedNames);
        Collections.sort(fullNames);
        Collections.sort(list);
        // some database engines to not honor setMaxFieldSize
        // mysql,...
        if ("MySQL".equals(getDatabaseName())) {
            assertEquals(fullNames, list);
        } else {
            assertEquals(truncatedNames, list);
        }
    }

    public void testQueryTimeout() throws Exception {
        // derby: does not honor queryTimeout
        // org.postgresql.util.PSQLException: Method org.postgresql.jdbc4.Jdbc4PreparedStatement.setQueryTimeout(int) is not yet implemented
        // skip this test
        if ("Apache Derby".equals(getDatabaseName())
                || "PostgreSQL".equals(getDatabaseName())) {
            return;
        }
        final Engine engine = getEngine();
        engine.execute(new CompiledSql(new StringSql("delete from big_table")), NO_OPTIONS);

        final Batcher batcher = engine.newBatcher(1000);

        for (int i=0; i<2000000; i++) {
            final int value = i;
            batcher.submit(new CompiledSql(new StringSql("insert into big_table (num) values(?)") {

                @Override
                public void setParameters(SqlParameters p) throws SQLException {
                    p.next().setInt(value);
                }
            }), NO_OPTIONS);
        }
        batcher.flush();

        final BigTable bigTable = new BigTable();
        final long start = System.currentTimeMillis();

        try {
            final List<Integer> list = bigTable.num.list(engine, Option.setQueryTimeout(1));
            fail("No timeout in " + (System.currentTimeMillis() - start) + " millis");
        } catch (SQLException e) {
            System.out.println("Timeout in "+ (System.currentTimeMillis() - start) + " millis");
            e.printStackTrace();
            // fine: timeout or timeout not supported
        }
    }

}
