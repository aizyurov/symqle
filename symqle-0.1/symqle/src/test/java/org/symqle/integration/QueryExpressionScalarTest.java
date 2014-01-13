package org.symqle.integration;

import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.TrueValue;
import org.symqle.sql.AbstractQueryExpressionBodyScalar;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class QueryExpressionScalarTest extends AbstractIntegrationTestBase {

    private AbstractQueryExpressionBodyScalar<Boolean> singleRowTrue() {
        final TrueValue trueValue = new TrueValue();
        return trueValue.value.distinct().union(trueValue.value.where(trueValue.value.eq(false)));
    }

    public void testBooleanValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(singleRowTrue().queryValue().asPredicate())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testExists() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(trueValue.value.distinct().exists())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testContains() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(trueValue.value.distinct().contains(true))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    /**
     * returns James, Bill, Alex, Margaret, James, Margaret (order undefined_
     * @param employee
     * @return
     */
    private AbstractQueryExpressionBodyScalar<String> firstNames(final Employee employee) {
        final Department department = new Department();
        return employee.firstName.distinct().unionAll(department.manager().firstName);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = firstNames(employee).unionAll(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "James", "Margaret", "Margaret"), list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = firstNames(employee).unionDistinct(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = firstNames(employee).union(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = firstNames(employee).exceptAll(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = firstNames(employee).exceptDistinct(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = firstNames(employee).except(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = firstNames(employee).intersectAll(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = firstNames(employee).intersectDistinct(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = firstNames(employee).intersect(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = firstNames(employee).forUpdate().list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret", "Margaret"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            // org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = firstNames(employee).forReadOnly().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret", "Margaret"), list);
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = firstNames(employee).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret", "Margaret"), list);
    }


}
