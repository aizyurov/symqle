package org.symqle.integration;

import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractQuerySpecificationScalar;
import org.symqle.sql.AbstractQueryTerm;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class QueryTermTest extends AbstractIntegrationTestBase {

    private boolean notApplicable;

    private AbstractQueryTerm<String> queryTerm(final Employee employee) {
        final Department department = new Department();
        return employee.lastName.where(employee.firstName.eq("James")).intersect(department.manager().lastName);
    }

    @Override
    protected void onSetUp() throws Exception {
        // skip databases, which do not support INTERSECT
        notApplicable = Arrays.asList("MySQL").contains(getDatabaseName());
    }

    public void testList() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    public void testForUpdate() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        try {
            final List<String> list = queryTerm(employee).forUpdate().list(getEngine());
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            // org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    public void testForReadOnly() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).forReadOnly().list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    public void testUnionAll() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).unionAll(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "First"), list);
    }

    public void testUnionDistinct() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).unionDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    public void testUnion() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).union(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    public void testExceptAll() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).exceptAll(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(), list);
    }

    public void testExceptDistinct() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).exceptDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(), list);
    }

    public void testExcept() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).except(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(), list);
    }

    public void testIntersectAll() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).intersectAll(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("First"), list);
    }

    public void testIntersectDistinct() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).intersectDistinct(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("First"), list);
    }

    public void testIntersect() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final List<String> list = queryTerm(employee).intersect(employee.lastName.where(employee.firstName.eq("James"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("First"), list);
    }

    public void testExists() throws Exception {
        // GenericDialect does not support Option.allowNoTables(true)
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final Department department = new Department();
        final AbstractQueryTerm<String> subquery = employee.lastName.where(employee.firstName.eq("James")).intersect(department.manager().lastName.where(department.deptName.eq("DEV")));
        final List<Integer> list = department.deptId.where(subquery.exists()).list(getEngine(), Option.allowNoTables(true));
        assertEquals(1, list.size());
    }

    public void testContains() throws Exception {
        // GenericDialect does not support Option.allowNoTables(true)
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final Department department = new Department();
        final AbstractQueryTerm<String> subquery = employee.lastName.where(employee.firstName.eq("James")).intersect(department.manager().lastName.where(department.deptName.eq("DEV")));
        final List<Integer> list = department.deptId.where(subquery.contains("First")).list(getEngine(), Option.allowNoTables(true));
        assertEquals(1, list.size());
    }

    public void testExistsNegative() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final Employee manager = new Employee();
        final Department department = new Department();
        final AbstractQueryTerm<String> subquery2 = employee.lastName.where(employee.firstName.eq("James")).intersect(manager.lastName.where(manager.empId.eq(department.manager().empId).and(department.deptName.eq("HR"))));
        final AbstractQuerySpecificationScalar<Integer> querySpec = department.deptId.where(subquery2.exists());
        System.out.println(querySpec.show(getEngine().getDialect()));
        final List<Integer> list2 = querySpec.list(getEngine());
        assertEquals(0, list2.size());
    }

    public void testQueryValue() throws Exception {
        // GenericDialect does not support Option.allowNoTables(true)
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final Department department = new Department();
        final AbstractQueryTerm<String> subquery = employee.lastName.where(employee.firstName.eq("James")).intersect(department.manager().lastName.where(department.deptName.eq("DEV")));
        final List<String> list = subquery.queryValue().list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList("First"), list);
    }

    public void testInArgument() throws Exception {
        if (notApplicable) {
            return;
        }
        final Employee employee = new Employee();
        final Employee another = new Employee();
        final List<Double> list = employee.salary.where(employee.lastName.in(queryTerm(another))).list(getEngine());
        assertEquals(Arrays.asList(3000.0), list);
    }

}
