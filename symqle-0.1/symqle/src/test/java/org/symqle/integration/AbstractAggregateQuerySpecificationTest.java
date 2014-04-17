package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractAggregateQuerySpecification;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractAggregateQuerySpecificationTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class AbstractAggregateQuerySpecificationTest extends AbstractIntegrationTestBase implements AbstractAggregateQuerySpecificationTestSet {

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).
                compileQuery(getEngine()).list();
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().where(employee.deptId.eq(department.deptId)).contains(5000.0))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_exists_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().where(employee.deptId.eq(department.deptId)).exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).forReadOnly().list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
            // it is unclear, what 'for update' means when selecting count. Lock the whole table? Nevertheless, some engines allow it.
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).forUpdate().list(getEngine());
            assertEquals(Arrays.asList(4), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            // org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE is not allowed with aggregate functions
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(Params.p(2).in(employee.empId.count().where(employee.deptId.eq(department.deptId))))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(Params.p(2).eq(employee.empId.count().where(employee.deptId.eq(department.deptId)).all()))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_any_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(Params.p(2).eq(employee.empId.count().where(employee.deptId.eq(department.deptId)).any()))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(Params.p(2).eq(employee.empId.count().where(employee.deptId.eq(department.deptId)).some()))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).
                limit(1).
                list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).
                limit(0, 1).
                list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(Params.p(2).notIn(employee.empId.count().where(employee.deptId.eq(department.deptId))))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Pair<String,Integer>> list = department.deptName
                .pair(
                        employee.empId.count().where(employee.salary.gt(2500.0).and(employee.deptId.eq(department.deptId))).queryValue()
                ).orderBy(department.deptName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("DEV", 1), Pair.make("HR", 1)), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final int iterations = employee.empId.count().where(employee.salary.gt(1800.0)).scroll(getEngine(), new Callback<Integer>() {
            @Override
            public boolean iterate(Integer integer) throws SQLException {
                assertEquals(4, integer.intValue());
                return true;
            }
        });
        assertEquals(1, iterations  );

    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final AbstractAggregateQuerySpecification<Integer> aggregateQuerySpecification =
                employee.empId.count().where(employee.salary.gt(1800.0));
        final String sql = aggregateQuerySpecification.showQuery(getEngine().getDialect());
        final Pattern pattern = Pattern.compile("SELECT COUNT\\(([A-Z][A-Z0-9]+)\\.emp_id\\) AS ([A-Z][A-Z0-9]+) FROM employee AS \\1 WHERE \\1.salary > \\?");
        assertTrue(sql, pattern.matcher(sql).matches());

    }

}
