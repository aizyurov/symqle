package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractQueryTerm;
import org.symqle.sql.Label;
import org.symqle.testset.AbstractQueryTermTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class QueryTermTest extends AbstractIntegrationTestBase implements AbstractQueryTermTestSet {

    private AbstractQueryTerm<String> queryTerm(final Employee employee) {
        final Department department = new Department();
        return employee.lastName.where(employee.firstName.eq("James")).intersect(department.manager().lastName);
    }

    @Override
    protected void runTest() throws Throwable {
        if (!getDatabaseName().equals(SupportedDb.MYSQL)) {
            super.runTest();
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).compileQuery(getEngine()).list();
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final AbstractQueryTerm<String> subquery = employee.lastName.where(employee.firstName.eq("James")).intersect(department.manager().lastName.where(department.deptName.eq("DEV")));
        final List<Integer> list = department.deptId.where(subquery.contains("First")).list(getEngine(), Option.allowNoTables(true));
        assertEquals(1, list.size());
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.where(employee.firstName.eq("James")).exceptAll(queryTerm(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = queryTerm(employee).exceptAll(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(), list);
        } catch (SQLException e) {
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.where(employee.firstName.eq("James")).exceptDistinct(queryTerm(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = queryTerm(employee).exceptDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(), list);
        } catch (SQLException e) {
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.H2);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James")).except(queryTerm(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).except(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(), list);
    }

    @Override
    public void test_exists_() throws Exception {
        final Employee noDeptEmployee = new Employee();
        final Employee departmentEmployee = new Employee();
        final Department department = new Department();
        final AbstractQueryTerm<String> subquery = noDeptEmployee.firstName.where(noDeptEmployee.deptId.isNull()).intersect(departmentEmployee.firstName.where(departmentEmployee.deptId.eq(department.deptId)));
        final List<String> list = department.deptName.where(subquery.exists()).list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).forReadOnly().list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = queryTerm(employee).forUpdate().list(getEngine());
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            // org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT
            // org.h2.jdbc.JdbcSQLException: Feature not supported: "FOR UPDATE && DISTINCT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee another = new Employee();
        final List<Double> list = employee.salary.where(employee.lastName.in(queryTerm(another))).list(getEngine());
        assertEquals(Arrays.asList(3000.0), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Employee employee = new Employee();
        final Employee another = new Employee();
        final List<Double> list = employee.salary.where(employee.lastName.eq(queryTerm(another).all())).list(getEngine());
        assertEquals(Arrays.asList(3000.0), list);
    }

    @Override
    public void test_any_() throws Exception {
        final Employee employee = new Employee();
        final Employee another = new Employee();
        final List<Double> list = employee.salary.where(employee.lastName.eq(queryTerm(another).any())).list(getEngine());
        assertEquals(Arrays.asList(3000.0), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Employee employee = new Employee();
        final Employee another = new Employee();
        final List<Double> list = employee.salary.where(employee.lastName.eq(queryTerm(another).some())).list(getEngine());
        assertEquals(Arrays.asList(3000.0), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = queryTerm(employee).intersectAll(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.where(employee.firstName.eq("James")).intersectAll(queryTerm(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = queryTerm(employee).intersectDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.where(employee.firstName.eq("James")).intersectDistinct(queryTerm(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).intersect(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James")).intersect(queryTerm(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).limit(1).list(getEngine());
        assertEquals(1, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).limit(1, 2).list(getEngine());
        assertEquals(0, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_countRows_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = queryTerm(employee).countRows().list(getEngine());
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee another = new Employee();
        final List<String> list = employee.lastName.where(employee.lastName.notIn(queryTerm(another)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final Label l = new Label();
        final List<String> list = employee.lastName.label(l).where(employee.firstName.eq("James"))
                .intersect(department.manager().lastName).orderBy(l).list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final Employee noDeptEmployee = new Employee();
        final Employee deptEmployee = new Employee();
        final Department department = new Department();
        final AbstractQueryTerm<String> subquery = noDeptEmployee.firstName.where(noDeptEmployee.deptId.isNull())
                .intersect(deptEmployee.firstName.where(deptEmployee.deptId.eq(department.deptId)));
        final List<String> list = subquery.queryValue().where(department.deptName.eq("DEV")).list(getEngine());
        assertEquals(Arrays.asList("James"), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final int count = queryTerm(employee).scroll(getEngine(), new Callback<String>() {
            @Override
            public boolean iterate(final String s) throws SQLException {
                assertEquals("First", s);
                return true;
            }
        });
        assertEquals(1, count);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = queryTerm(employee).showQuery(getEngine().getDialect());
        final Pattern expected;
        expected = Pattern.compile("SELECT ([A-Z][A-Z0-9]*)\\.last_name AS [A-Z][A-Z0-9]* FROM employee AS \\1" +
                " WHERE \\1\\.first_name = \\?" +
                " INTERSECT SELECT ([A-Z][A-Z0-9]*)\\.last_name AS [A-Z][A-Z0-9]* FROM department AS ([A-Z][A-Z0-9]*)" +
                " LEFT JOIN employee AS \\2 ON \\2\\.emp_id = \\3\\.manager_id");
        assertTrue(sql, expected.matcher(sql).matches());
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).unionAll(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "First"), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James")).unionAll(queryTerm(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "First"), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).unionDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James")).unionDistinct(queryTerm(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).union(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James")).union(queryTerm(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }


}
