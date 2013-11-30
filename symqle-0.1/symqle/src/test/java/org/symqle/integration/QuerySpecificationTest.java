package org.symqle.integration;

import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.dialect.MySqlDialect;
import org.symqle.sql.AbstractQuerySpecification;
import org.symqle.sql.AbstractQuerySpecificationScalar;
import org.symqle.sql.Dialect;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class QuerySpecificationTest extends AbstractIntegrationTestBase {

    private AbstractQuerySpecificationScalar<String> querySpec(final Employee employee) {
        return employee.lastName.where(employee.salary.gt(2500.0));
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).forReadOnly().list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First", "Redwood"), list);
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

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).unionAll(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "First", "Redwood"), list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).unionDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = querySpec(employee).union(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).exceptAll(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).exceptDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).except(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).intersectAll(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).intersectDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = querySpec(employee).intersect(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExists() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // subquery: all employees of any department, which have the same first name of this dept manager but the manager himself
        final AbstractQuerySpecificationScalar<Integer> subquery = employee.empId.where(department.manager().firstName.eq(employee.firstName).and(department.manager().empId.ne(employee.empId)));
        final List<String> list = department.deptName.where(subquery.exists()).list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testContains() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // subquery: all employees of any department, which have the same first name of this dept manager but the manager himself
        final AbstractQuerySpecificationScalar<String> subquery = employee.lastName.where(department.manager().firstName.eq(employee.firstName).and(department.manager().empId.ne(employee.empId)));
        final List<String> list = department.deptName.where(subquery.contains("Cooper")).list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testQueryValue() throws Exception {
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
