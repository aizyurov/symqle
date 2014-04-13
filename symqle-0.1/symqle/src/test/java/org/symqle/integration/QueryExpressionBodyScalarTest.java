package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.TrueValue;
import org.symqle.sql.AbstractQueryExpressionBodyScalar;
import org.symqle.sql.Label;
import org.symqle.testset.AbstractQueryExpressionBodyScalarTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class QueryExpressionBodyScalarTest extends AbstractIntegrationTestBase implements AbstractQueryExpressionBodyScalarTestSet {

    private AbstractQueryExpressionBodyScalar<Boolean> singleRowTrue() {
        final TrueValue trueValue = new TrueValue();
        return trueValue.value.distinct().union(trueValue.value.where(trueValue.value.eq(false)));
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = firstNames(employee).compileQuery(getEngine()).list();
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret", "Margaret"), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final Employee employee = new Employee();
        final AbstractQueryExpressionBodyScalar<Boolean> union = trueValue.value.union(trueValue.value);
        final List<String> list = employee.lastName.where(union.contains(true))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final Department department = new Department();
        try {
            final AbstractQueryExpressionBodyScalar<String> cooperAndManagers = cooper.firstName.where(cooper.lastName.eq("Cooper")).union(department.manager().firstName);
            final List<String> list = employee.firstName.exceptAll(cooperAndManagers).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = firstNames(employee).exceptAll(cooper.firstName.where(cooper.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final Department department = new Department();
        try {
            final AbstractQueryExpressionBodyScalar<String> cooperAndManagers = cooper.firstName.where(cooper.lastName.eq("Cooper")).union(department.manager().firstName);
            final List<String> list = employee.firstName.exceptDistinct(cooperAndManagers).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = firstNames(employee).exceptDistinct(cooper.firstName.where(cooper.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final Department department = new Department();
        try {
            final AbstractQueryExpressionBodyScalar<String> cooperAndManagers = cooper.firstName.where(cooper.lastName.eq("Cooper")).union(department.manager().firstName);
            final List<String> list = employee.firstName.except(cooperAndManagers).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = firstNames(employee).except(cooper.firstName.where(cooper.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(trueValue.value.union(trueValue.value).exists())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = firstNames(employee).forReadOnly().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret", "Margaret"), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = firstNames(employee).forUpdate().list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret", "Margaret"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            // org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee staff = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.in(firstNames(staff))).orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = firstNames(employee).intersectAll(cooper.firstName.where(cooper.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final Department department = new Department();
        try {
            final AbstractQueryExpressionBodyScalar<String> cooperAndManagers = cooper.firstName.where(cooper.lastName.eq("Cooper")).union(department.manager().firstName);
            final List<String> list = employee.firstName.intersectAll(cooperAndManagers).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = firstNames(employee).intersectDistinct(cooper.firstName.where(cooper.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final Department department = new Department();
        try {
            final AbstractQueryExpressionBodyScalar<String> cooperAndManagers = cooper.firstName.where(cooper.lastName.eq("Cooper")).union(department.manager().firstName);
            final List<String> list = employee.firstName.intersectDistinct(cooperAndManagers).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = firstNames(employee).intersect(cooper.firstName.where(cooper.lastName.eq("Cooper"))).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final Department department = new Department();
        try {
            final AbstractQueryExpressionBodyScalar<String> cooperAndManagers = cooper.firstName.where(cooper.lastName.eq("Cooper")).union(department.manager().firstName);
            final List<String> list = employee.firstName.intersect(cooperAndManagers).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = firstNames(employee).limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = firstNames(employee).limit(2, 5).list(getEngine());
        assertEquals(4, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = firstNames(employee).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret", "Margaret"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final Department department = new Department();
        final AbstractQueryExpressionBodyScalar<String> cooperAndManagers =
                cooper.firstName.where(cooper.lastName.eq("Cooper"))
                        .union(department.manager().firstName);
        final List<String> list = employee.lastName.where(employee.firstName.notIn(cooperAndManagers)).orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);

    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee cooper = new Employee();
        final Department department = new Department();
        final Label label = new Label();
        final AbstractQueryExpressionBodyScalar<String> cooperAndManagers =
                cooper.firstName.label(label).where(cooper.lastName.eq("Cooper"))
                        .union(department.manager().firstName);
        final List<String> list = cooperAndManagers.orderBy(label).list(getEngine());
        assertEquals(Arrays.asList("James", "Margaret"), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final Employee cooper = new Employee();
        final List<Pair<String,Boolean>> list = cooper.lastName
                .pair(trueValue.value.union(trueValue.value.where(trueValue.value.eq(false))).queryValue())
                .where(cooper.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("Cooper", true), list.get(0));


    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected = new ArrayList<>(Arrays.asList("Alex", "Bill", "James", "James", "Margaret", "Margaret"));
        final int count = firstNames(employee).scroll(getEngine(), new Callback<String>() {
            @Override
            public boolean iterate(final String s) throws SQLException {
                assertTrue(expected + " does not contain " + s, expected.remove(s));
                return true;
            }
        });
        assertEquals(6, count);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = firstNames(employee).showQuery(getEngine().getDialect());
        Pattern expected;
        expected = Pattern.compile("SELECT DISTINCT ([A-Z][A-Z0-9]*)\\.first_name AS [A-Z][A-Z0-9]* FROM employee AS \\1" +
                " UNION ALL SELECT ([A-Z][A-Z0-9]*)\\.first_name AS [A-Z][A-Z0-9]* FROM department AS ([A-Z][A-Z0-9]*)" +
                " LEFT JOIN employee AS \\2 ON \\2\\.emp_id = \\3\\.manager_id");
        assertTrue(sql, expected.matcher(sql).matches());
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Employee cooper = new Employee();
            final List<String> list = cooper.firstName.where(cooper.lastName.eq("Cooper")).unionAll(firstNames(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "James", "Margaret", "Margaret"), list);
        } catch (SQLException e) {
            // MySQL bug
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<String> list = firstNames(employee).unionAll(cooper.firstName.where(cooper.lastName.eq("Cooper"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "James", "Margaret", "Margaret"), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Employee cooper = new Employee();
            final List<String> list = cooper.firstName.where(cooper.lastName.eq("Cooper")).unionDistinct(firstNames(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
        } catch (SQLException e) {
            // MySQL bug
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<String> list = firstNames(employee).unionDistinct(cooper.firstName.where(cooper.lastName.eq("Cooper"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Employee cooper = new Employee();
            final List<String> list = cooper.firstName.where(cooper.lastName.eq("Cooper")).union(firstNames(employee)).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
        } catch (SQLException e) {
            // MySQL bug
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<String> list = firstNames(employee).union(cooper.firstName.where(cooper.lastName.eq("Cooper"))).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    /**
     * returns James, Bill, Alex, Margaret, James, Margaret (order undefined)
     * @param employee
     * @return
     */
    private AbstractQueryExpressionBodyScalar<String> firstNames(final Employee employee) {
        final Department department = new Department();
        return employee.firstName.distinct().unionAll(department.manager().firstName);
    }


}
