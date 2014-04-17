package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.MyDual;
import org.symqle.sql.AbstractSelectSublist;
import org.symqle.sql.Label;
import org.symqle.testset.AbstractSelectSublistTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class SelectSublistTest extends AbstractIntegrationTestBase implements AbstractSelectSublistTestSet {

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.compileQuery(getEngine()).list();
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final MyDual myDual = new MyDual();
        final AbstractSelectSublist<String> dummy = AbstractSelectSublist.adapt(myDual.dummy);
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(dummy.contains("X").and(employee.lastName.eq("Redwood")))
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);

    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.distinct().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = department.manager().salary.label(label);
            final List<Double> list = employee.salary.exceptAll(selectSublist).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
            final List<Double> list = selectSublist.exceptAll(department.manager().salary).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = department.manager().salary.label(label);
            final List<Double> list = employee.salary.exceptDistinct(selectSublist).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(1500.0, 2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
            final List<Double> list = selectSublist.exceptDistinct(department.manager().salary).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(1500.0, 2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = department.manager().salary.label(label);
            final List<Double> list = employee.salary.except(selectSublist).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(1500.0, 2000.0), list);
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
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
            final List<Double> list = selectSublist.except(department.manager().salary).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(1500.0, 2000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final MyDual myDual = new MyDual();
        final Label label = new Label();
        final AbstractSelectSublist<String> dummy = AbstractSelectSublist.adapt(myDual.dummy);
        final Employee employee = new Employee();
            final List<String> list = employee.lastName.where(dummy.exists().and(employee.lastName.eq("Redwood")))
                    .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.forReadOnly().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final AbstractSelectSublist<Double> selectSublist = AbstractSelectSublist.adapt(department.manager().salary);
        final List<String> list = employee.lastName.where(employee.salary.in(selectSublist))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final AbstractSelectSublist<Double> selectSublist = AbstractSelectSublist.adapt(department.manager().salary);
        final List<String> list = employee.lastName.where(employee.salary.eq(selectSublist.all()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_any_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final AbstractSelectSublist<Double> selectSublist = AbstractSelectSublist.adapt(department.manager().salary);
        final List<String> list = employee.lastName.where(employee.salary.eq(selectSublist.any()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final AbstractSelectSublist<Double> selectSublist = AbstractSelectSublist.adapt(department.manager().salary);
        final List<String> list = employee.lastName.where(employee.salary.eq(selectSublist.some()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
            final List<Double> list = selectSublist.intersectAll(department.manager().salary).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(3000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = department.manager().salary.label(label);
            final List<Double> list = employee.salary.intersectAll(selectSublist).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(3000.0, 3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
            final List<Double> list = selectSublist.intersectDistinct(department.manager().salary).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = department.manager().salary.label(label);
            final List<Double> list = employee.salary.intersectDistinct(selectSublist).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
            final List<Double> list = selectSublist.intersect(department.manager().salary).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(3000.0), list);
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
            final Label label = new Label();
            final AbstractSelectSublist<Double> selectSublist = department.manager().salary.label(label);
            final List<Double> list = employee.salary.intersectDistinct(selectSublist).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.limit(2, 10).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = AbstractSelectSublist.adapt(department.manager().salary);
        final List<String> list = employee.lastName.where(employee.salary.notIn(selectSublist))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.orderBy(label).list(getEngine());
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Pair<Double,String>> list = selectSublist.pair(employee.lastName)
                .where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make(3000.0, "Redwood"), list.get(0));
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Pair<String, Double>> list = employee.lastName.pair(selectSublist)
                .where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("Redwood", 3000.0), list.get(0));
    }

    @Override
    public void test_queryValue_() throws Exception {
        final MyDual myDual = new MyDual();
        final Label label = new Label();
        final AbstractSelectSublist<String> dummy = AbstractSelectSublist.adapt(myDual.dummy);
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.lastName.pair(dummy.queryValue())
                .where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("Redwood", "X"), list.get(0));
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> expected = new ArrayList<>(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0));
        selectSublist.forUpdate().scroll(getEngine(), new Callback<Double>() {
            @Override
            public boolean iterate(final Double aDouble) throws SQLException {
                assertTrue(expected.toString(), expected.remove(aDouble));
                return true;
            }
        });
        assertTrue(expected.toString(), expected.isEmpty());
    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.selectAll().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final String sql = selectSublist.showQuery(getEngine().getDialect());
        Pattern expected = Pattern.compile("SELECT ([A-Z][A-Z0-9]*).salary AS [A-Z][A-Z0-9]* FROM employee AS \\1");
        assertTrue(sql, expected.matcher(sql).matches());
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = department.manager().salary.label(label);
        final List<Double> list = employee.salary.unionAll(selectSublist).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.unionAll(department.manager().salary).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0, 3000.0, 3000.0), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = department.manager().salary.label(label);
        final List<Double> list = employee.salary.unionDistinct(selectSublist).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.unionDistinct(department.manager().salary).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = department.manager().salary.label(label);
        final List<Double> list = employee.salary.union(selectSublist).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.union(department.manager().salary).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label label = new Label();
        final AbstractSelectSublist<Double> selectSublist = employee.salary.label(label);
        final List<Double> list = selectSublist.where(employee.lastName.eq("Cooper")).list(getEngine());
        assertEquals(Arrays.asList(1500.0), list);
    }
}
