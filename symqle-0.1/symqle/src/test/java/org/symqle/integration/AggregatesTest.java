package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractAggregateFunction;
import org.symqle.sql.AggregateFunction;
import org.symqle.testset.AbstractAggregateFunctionTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lvovich
 */
public class AggregatesTest extends AbstractIntegrationTestBase  implements AbstractAggregateFunctionTestSet {

    @Override
    public void test_adapt_AggregateFunction() throws Exception {
        final Employee employee = new Employee();
        final AggregateFunction<Integer> count = employee.empId.count();
        final List<Integer> list = AbstractAggregateFunction.adapt(count).list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().compileQuery(getEngine()).list();
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName.where(employee.empId.count().contains(5)).
                orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_exists_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().forReadOnly().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
            // it is unclear, what 'for update' means when selecting count. Lock the whole table? Nevertheless, some engines allow it.
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().forUpdate().list(getEngine());
            assertEquals(Arrays.asList(5), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            // org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE is not allowed with aggregate functions
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee allEmployees = new Employee();
        final List<String> list = employee.lastName.where(employee.empId.in(allEmployees.deptId.count())).list(getEngine());
        assertEquals(Arrays.asList("Pedersen"), list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().limit(1).list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().limit(1, 1).list(getEngine());
        assertEquals(Arrays.asList(), list);
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee allEmployees = new Employee();
        final List<String> list = employee.lastName.where(employee.empId.notIn(allEmployees.deptId.count())).
                orderBy(employee.lastName).
                list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Redwood"), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Pair<String,Integer>> list = department.deptName
                .pair(
                        employee.empId.count().queryValue()
                ).orderBy(department.deptName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("DEV", 5), Pair.make("HR", 5)), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final AtomicInteger callCount = new AtomicInteger();
        final int iterations = employee.empId.count().scroll(getEngine(), new Callback<Integer>() {
            @Override
            public boolean iterate(final Integer integer) throws SQLException {
                assertEquals(callCount.getAndIncrement(), 0);
                assertEquals(integer.intValue(), 5);
                return true;
            }
        });
        assertEquals(iterations, 1);
        assertEquals(callCount.get(), 1);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = employee.empId.count().showQuery(getEngine().getDialect());
        // duplicates core test;
        // better assert there
        assertTrue(sql, sql.startsWith("SELECT COUNT("));
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

}
