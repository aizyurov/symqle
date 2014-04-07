package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.MyDual;
import org.symqle.sql.AbstractQuerySpecificationScalar;
import org.symqle.testset.AbstractQuerySpecificationScalarTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class QuerySpecificationScalarTest extends AbstractIntegrationTestBase implements AbstractQuerySpecificationScalarTestSet {

    private AbstractQuerySpecificationScalar<String> querySpec(final Employee employee) {
        return employee.lastName.where(employee.salary.gt(2500.0));
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).compileQuery(getEngine()).list();
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // subquery: all employees of any department, which have the same first name of this dept manager but the manager himself
        final AbstractQuerySpecificationScalar<String> subquery = employee.lastName.where(department.manager().firstName.eq(employee.firstName).and(department.manager().empId.ne(employee.empId)));
        final List<String> list = department.deptName.where(subquery.contains("Cooper")).list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee allEmployees = new Employee();
        try {
            final List<String> list = allEmployees.lastName.exceptAll(querySpec(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        try {
            final List<String> list = querySpec(employee).exceptAll(james.lastName.where(james.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee allEmployees = new Employee();
        try {
            final List<String> list = allEmployees.lastName.exceptDistinct(querySpec(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        try {
            final List<String> list = querySpec(employee).exceptDistinct(james.lastName.where(james.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        try {
            final List<String> list = querySpec(employee).except(james.lastName.where(james.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        try {
            final List<String> list = querySpec(employee).except(james.lastName.where(james.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // subquery: all employees of any department, which have the same first name of this dept manager but the manager himself
        final AbstractQuerySpecificationScalar<Integer> subquery = employee.empId.where(department.manager().firstName.eq(employee.firstName).and(department.manager().empId.ne(employee.empId)));
        final List<String> list = department.deptName.where(subquery.exists()).list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
            final List<String> list = querySpec(employee).forReadOnly().list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee overpaid = new Employee();

        final AbstractQuerySpecificationScalar<String> overpaidNames =
                overpaid.firstName.where(overpaid.salary.gt(2500.0));
        final List<String> list = employee.lastName
                .where(employee.firstName.in(overpaidNames))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);

    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        try {
            final List<String> list = querySpec(employee).intersectAll(james.lastName.where(james.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Employee allEmployees = new Employee();
        try {
            final List<String> list = allEmployees.lastName.intersectAll(querySpec(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First", "Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        try {
            final List<String> list = querySpec(employee).intersectDistinct(james.lastName.where(james.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Employee allEmployees = new Employee();
        try {
            final List<String> list = allEmployees.lastName.intersectDistinct(querySpec(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First", "Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        try {
            final List<String> list = querySpec(employee).intersect(james.lastName.where(james.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Employee allEmployees = new Employee();
        try {
            final List<String> list = allEmployees.lastName.intersect(querySpec(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First", "Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).limit(1).list(getEngine());
        assertEquals(1, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).limit(1, 5).list(getEngine());
        assertEquals(1, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee overpaid = new Employee();

        final AbstractQuerySpecificationScalar<String> overpaidNames =
                overpaid.firstName.where(overpaid.salary.gt(2500.0));
        final List<String> list = employee.lastName
                .where(employee.firstName.notIn(overpaidNames))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final MyDual myDual = new MyDual();
        final AbstractQuerySpecificationScalar<String> querySpec = myDual.dummy.where(myDual.dummy.isNotNull());
        final Employee employee = new Employee();
        final List<Pair<String,String>> list = employee.lastName.pair(querySpec.queryValue())
                .where(employee.lastName.eq("Cooper")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("Cooper", "X"), list.get(0));

    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected = new ArrayList<>(Arrays.asList("First", "Redwood"));
        final int count = querySpec(employee).scroll(getEngine(), new Callback<String>() {
            @Override
            public boolean iterate(final String s) throws SQLException {
                assertTrue(expected + " does not contain " + s, expected.remove(s));
                return true;
            }
        });
        assertEquals(2, count);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = querySpec(employee).showQuery(getEngine().getDialect());
        final Pattern expected;
        expected = Pattern.compile("SELECT ([A-Z][A-Z0-9]*)\\.last_name AS [A-Z][A-Z0-9]* FROM employee AS \\1 WHERE \\1\\.salary > \\?");
        assertTrue(sql, expected.matcher(sql).matches());
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final List<String> list = querySpec(employee).unionAll(james.lastName.where(james.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "First", "Redwood"), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final List<String> list = james.lastName.where(james.firstName.eq("James")).unionAll(querySpec(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "First", "Redwood"), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final List<String> list = querySpec(employee).unionDistinct(james.lastName.where(james.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final List<String> list = james.lastName.where(james.firstName.eq("James")).unionDistinct(querySpec(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final List<String> list = querySpec(employee).union(james.lastName.where(james.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final List<String> list = james.lastName.where(james.firstName.eq("James")).union(querySpec(employee)).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testQueryValueInDepth() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // subquery: all employees, which have the same first name of dept manager but the managet himself
        // for "DEV" department there is only one, so it may be used for queryValue
        final AbstractQuerySpecificationScalar<String> subquery = employee.lastName.where(department.manager().firstName.eq(employee.firstName).and(department.manager().empId.ne(employee.empId)));
        final List<String> list = subquery.queryValue().where(department.deptName.eq("DEV")).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);

        // for "HR" department there are none - depending of database it may cause SQLException or return null
        final List<String> nothing = subquery.queryValue().where(department.deptName.eq("HR")).list(getEngine());
        assertEquals(1, nothing.size());
        assertEquals(null, nothing.get(0));

    }



}
