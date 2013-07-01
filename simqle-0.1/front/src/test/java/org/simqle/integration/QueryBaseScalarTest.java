package org.simqle.integration;

import org.simqle.integration.model.Employee;
import org.simqle.integration.model.TrueValue;
import org.simqle.mysql.MySqlDialect;
import org.simqle.sql.AbstractQueryBaseScalar;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class QueryBaseScalarTest extends AbstractIntegrationTestBase {

    private AbstractQueryBaseScalar<Boolean> singleRowTrue() {
        final TrueValue trueValue = new TrueValue();
        return trueValue.value.distinct();
    }

    public void testBooleanValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(singleRowTrue().queryValue().booleanValue())
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testExists() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(trueValue.value.distinct().exists())
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testContains() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(trueValue.value.distinct().contains(true))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    private AbstractQueryBaseScalar<String> distinctFirstNames(final Employee employee) {
        return employee.firstName.distinct();
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).unionAll(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret"), list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).unionDistinct(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).union(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = distinctFirstNames(employee).exceptAll(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = distinctFirstNames(employee).exceptDistinct(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
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
            final List<String> list = distinctFirstNames(employee).except(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
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
            final List<String> list = distinctFirstNames(employee).intersectAll(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
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
            final List<String> list = distinctFirstNames(employee).intersectDistinct(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
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
            final List<String> list = distinctFirstNames(employee).intersect(employee.firstName.where(employee.lastName.eq("Cooper"))).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("James"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).where(employee.department().deptName.eq("DEV"))
                .orderBy(employee.firstName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Alex", "James"), list);
    }

    public void testWrongOrderBy() throws Exception {
        // order by a column, which does not appear in DISTINCT select list, is ambiguous:
        // which James should we keep - Cooper (1st) of First(2nd)?
        final Employee employee = new Employee();
        try {
            final List<String> list = distinctFirstNames(employee).orderBy(employee.lastName).list(getDatabaseGate());
            assertEquals(Arrays.asList("James", "Bill", "Alex", "Margaret"), list);
        } catch (SQLException e) {
            // derby: ERROR 42879: The ORDER BY clause may not contain column 'LAST_NAME', since the query specifies DISTINCT and that column does not appear in the query result
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).orderBy(employee.firstName).list(getDatabaseGate());
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    public void testOrderAsc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).orderAsc().list(getDatabaseGate());
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    public void testOrderDesc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).orderDesc().list(getDatabaseGate());
        assertEquals(Arrays.asList("Margaret", "James", "Bill", "Alex"), list);
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = distinctFirstNames(employee).forUpdate().list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = distinctFirstNames(employee).forReadOnly().list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
        } catch (SQLException e) {
            if (MySqlDialect.class.equals(getDatabaseGate().getDialect().getClass())) {
                // should work with MySqlDialect
                throw e;
            } else {
                // mysql does not support FOR READ ONLY natively
                expectSQLException(e, "MySQL");
            }
        }
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }


}
