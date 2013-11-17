package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.dialect.MySqlDialect;
import org.symqle.generic.Params;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.sql.Dialect;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
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

    public void testOrderAsc() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).orderAsc().list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    public void testOrderDesc() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).orderDesc().list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).union(employee.empId.count()).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(4, 5), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).unionAll(employee.empId.count().where(employee.salary.gt(1900.0))).list(getEngine());
        assertEquals(Arrays.asList(4, 4), list);
    }
    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).unionDistinct(employee.empId.count().where(employee.salary.gt(1900.0))).list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).except(employee.empId.count().where(employee.salary.gt(1900.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).exceptAll(employee.empId.count().where(employee.salary.gt(1900.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).exceptDistinct(employee.empId.count().where(employee.salary.gt(1900.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).intersect(employee.empId.count().where(employee.salary.lt(1800.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).intersectAll(employee.empId.count().where(employee.salary.lt(1800.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).intersectDistinct(employee.empId.count().where(employee.salary.lt(1800.0))).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }


    public void testForUpdate() throws Exception {
            // it is unclear, what 'for update' means when selecting count. Lock the whole table? Nevertheless, some engines allow it.
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).forUpdate().list(getEngine());
            assertEquals(Arrays.asList(4), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().where(employee.salary.gt(1800.0)).forReadOnly().list(getEngine());
            assertEquals(Arrays.asList(4), list);
        } catch (SQLException e) {
            if (MySqlDialect.class.equals(getEngine().initialContext().get(Dialect.class).getClass())) {
                // should work with MySqlDialect
                throw e;
            } else {
                // mysql does not support FOR READ ONLY natively
                expectSQLException(e, "MySQL");
            }
        }
    }

    public void testExists() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().where(employee.deptId.eq(department.deptId)).exists())
                .orderAsc()
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testContains() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().where(employee.deptId.eq(department.deptId)).contains(5000.0))
                .orderAsc()
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testIn() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(Params.p(2).in(employee.empId.count().where(employee.deptId.eq(department.deptId))))
                .orderAsc()
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
