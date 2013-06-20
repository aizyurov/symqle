package org.simqle.integration;

import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.mysql.MysqlDialect;
import org.simqle.sql.AbstractQuerySpecification;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class QuerySpecificationTest extends AbstractIntegrationTestBase {

    private AbstractQuerySpecification<String> querySpec(final Employee employee) {
        return employee.lastName.where(employee.salary.gt(2500.0));
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).orderBy(employee.lastName).list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).forUpdate().list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).forReadOnly().list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("First", "Redwood"), list);
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

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).unionAll(employee.lastName.where(employee.firstName.eq("James"))).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "First", "Redwood"), list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).unionDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).union(employee.lastName.where(employee.firstName.eq("James"))).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).exceptAll(employee.lastName.where(employee.firstName.eq("James"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).exceptDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).except(employee.lastName.where(employee.firstName.eq("James"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).intersectAll(employee.lastName.where(employee.firstName.eq("James"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).intersectDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).intersect(employee.lastName.where(employee.firstName.eq("James"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testExists() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // subquery: all employees of this department, which have the same first name of dept manager but the managet himself
        final AbstractQuerySpecification<Integer> subquery = employee.empId.where(department.manager().firstName.eq(employee.firstName).and(department.manager().empId.ne(employee.empId)));
        final List<String> list = department.deptName.where(subquery.exists()).list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testQueryValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // subquery: all employees, which have the same first name of dept manager but the managet himself
        // for "DEV" department there is only one, so it may be used for queryValue
        final AbstractQuerySpecification<String> subquery = employee.lastName.where(department.manager().firstName.eq(employee.firstName).and(department.manager().empId.ne(employee.empId)));
        final List<String> list = subquery.queryValue().where(department.deptName.eq("DEV")).list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper"), list);

        // for "HR" department there are none - depending of database it may cause SQLException or return null
        final List<String> nothing = subquery.queryValue().where(department.deptName.eq("HR")).list(getDatabaseGate());
        assertEquals(1, nothing.size());
        assertEquals(null, nothing.get(0));

    }



}
