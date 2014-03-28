package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.sql.Params;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class AggregateQueryExpressionTest extends AbstractIntegrationTestBase {

    public void testSelect() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    public void testForUpdate() throws Exception {
            // it is unclear, what 'for update' means when selecting count. Lock the whole table? Nevertheless, some engines allow it.
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).forUpdate().list(getEngine());
            assertEquals(Arrays.asList(4), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            // org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE is not allowed with aggregate functions
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).forReadOnly().list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    public void testExists() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().where(employee.deptId.eq(department.deptId)).exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testContains() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().where(employee.deptId.eq(department.deptId)).contains(5000.0))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testIn() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(Params.p(2).in(employee.empId.count().where(employee.deptId.eq(department.deptId))))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testQueryValue() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Pair<String,Integer>> list = department.deptName
                .pair(
                        employee.empId.count().where(employee.salary.gt(2500.0).and(employee.deptId.eq(department.deptId))).queryValue()
                ).orderBy(department.deptName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("DEV", 1), Pair.make("HR", 1)), list);
    }


}
