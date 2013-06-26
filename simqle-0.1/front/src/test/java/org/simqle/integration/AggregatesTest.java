package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.mysql.MysqlDialect;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class AggregatesTest extends AbstractIntegrationTestBase {

    public void testSelect() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().list(getDatabaseGate());
        assertEquals(Arrays.asList(5), list);
    }

    public void testOrderAsc() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().orderAsc().list(getDatabaseGate());
        assertEquals(Arrays.asList(5), list);
    }

    public void testOrderDesc() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().orderDesc().list(getDatabaseGate());
        assertEquals(Arrays.asList(5), list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().union(employee.empId.count().where(employee.salary.gt(1800.0))).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList(4, 5), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().unionAll(employee.empId.count().where(employee.salary.gt(100.0))).list(getDatabaseGate());
        assertEquals(Arrays.asList(5, 5), list);
    }
    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.empId.count().unionDistinct(employee.empId.count().where(employee.salary.gt(100.0))).list(getDatabaseGate());
        assertEquals(Arrays.asList(5), list);
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().except(employee.empId.count().where(employee.salary.gt(100.0))).list(getDatabaseGate());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().exceptAll(employee.empId.count().where(employee.salary.gt(100.0))).list(getDatabaseGate());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().exceptDistinct(employee.empId.count().where(employee.salary.gt(100.0))).list(getDatabaseGate());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().intersect(employee.empId.count().where(employee.salary.gt(1800.0))).list(getDatabaseGate());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().intersectAll(employee.empId.count().where(employee.salary.lt(1800.0))).list(getDatabaseGate());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().intersectDistinct(employee.empId.count().where(employee.salary.lt(1800.0))).list(getDatabaseGate());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }


    public void testForUpdate() throws Exception {
            // it is unclear, what 'for update' means when selecting count. Lock the whole table? Nevertheless, some engines allow it.
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().forUpdate().list(getDatabaseGate());
            assertEquals(Arrays.asList(5), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            expectSQLException(e, "derby");
        }
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = employee.empId.count().forReadOnly().list(getDatabaseGate());
            assertEquals(Arrays.asList(5), list);
        } catch (SQLException e) {
            if (MysqlDialect.class.equals(getDatabaseGate().getDialect().getClass())) {
                // should work with MysqlDialect
                throw e;
            } else {
                // mysql does not support FOR READ ONLY natively
                expectSQLException(e, "mysql");
            }
        }
    }

    public void testExists() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().exists())
                .orderAsc()
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testContains() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.salary.sum().contains(11500.0))
                .orderAsc()
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testIn() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(Params.p(5).in(employee.empId.count()))
                .orderAsc()
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testQueryValue() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Pair<String,Integer>> list = department.deptName
                .pair(
                        employee.empId.count().queryValue()
                ).orderBy(department.deptName).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make("DEV", 5), Pair.make("HR", 5)), list);
    }


}
