package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.TrueValue;
import org.symqle.sql.AbstractQueryBaseScalar;
import org.symqle.sql.Column;
import org.symqle.testset.AbstractQueryBaseScalarTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class QueryBaseScalarTest extends AbstractIntegrationTestBase implements AbstractQueryBaseScalarTestSet {

    private AbstractQueryBaseScalar<Boolean> singleRowTrue() {
        final TrueValue trueValue = new TrueValue();
        return trueValue.value.distinct();
    }

    public void test_adapt_QueryBaseScalar() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final Column<Boolean> value = trueValue.value;
        final AbstractQueryBaseScalar<Boolean> adaptor = AbstractQueryBaseScalar.adapt(value);
        final List<Boolean> list = adaptor.list(getEngine());
        assertEquals(Arrays.asList(true), list);
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).compileQuery(getEngine()).list();
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(trueValue.value.distinct().contains(true))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).exceptAll(allFirstNames(department.manager())).list(getEngine());
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
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).exceptAll(allFirstNames(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).exceptDistinct(allFirstNames(department.manager())).list(getEngine());
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
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).exceptDistinct(allFirstNames(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).except(allFirstNames(department.manager())).list(getEngine());
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
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).except(allFirstNames(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(trueValue.value.distinct().exists())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).forReadOnly().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).forReadOnly().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.in(distinctFirstNames(department.manager())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.lt(distinctFirstNames(department.manager()).all()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_any_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.ne(distinctFirstNames(department.manager()).any()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.lt(distinctFirstNames(department.manager()).some()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).intersectAll(allFirstNames(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).intersectAll(allFirstNames(department.manager())).list(getEngine());
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
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).intersectDistinct(allFirstNames(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).intersectDistinct(allFirstNames(department.manager())).list(getEngine());
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
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).intersect(allFirstNames(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = allFirstNames(employee).intersectDistinct(allFirstNames(department.manager())).list(getEngine());
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
        final List<String> list = distinctFirstNames(employee)
                .limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee)
                .limit(2, 5).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.notIn(distinctFirstNames(department.manager())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee)
                .orderBy(employee.firstName)
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final TrueValue trueValue = new TrueValue();
        final AbstractQueryBaseScalar<Boolean> queryBaseScalar = trueValue.value.distinct();
        final Employee employee = new Employee();
        final List<Pair<String,Boolean>> list = employee.lastName.pair(queryBaseScalar.queryValue()).where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("Cooper", true), list.get(0));

    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected = new ArrayList<>(Arrays.asList("Alex", "Bill", "James", "Margaret"));
        final int callCount = distinctFirstNames(employee).scroll(getEngine(), new Callback<String>() {
            @Override
            public boolean iterate(final String s) throws SQLException {
                assertTrue(expected + " does not contain " + s, expected.remove(s));
                return true;
            }
        });
        assertEquals(4, callCount);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = distinctFirstNames(employee).showQuery(getEngine().getDialect());
        final Pattern expected = Pattern.compile("SELECT DISTINCT ([A-Z][A-Z0-9]*).first_name AS [A-Z][A-Z0-9]* FROM employee AS \\1");
        assertTrue(sql, expected.matcher(sql).matches());

    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = allFirstNames(employee).unionAll(allFirstNames(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "James", "Margaret", "Margaret"), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = allFirstNames(employee).unionAll(allFirstNames(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "James", "Margaret", "Margaret"), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = allFirstNames(employee).unionDistinct(allFirstNames(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = allFirstNames(employee).unionDistinct(allFirstNames(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = allFirstNames(employee).unionDistinct(allFirstNames(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = allFirstNames(employee).unionDistinct(allFirstNames(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = distinctFirstNames(employee)
                .where(employee.department().deptName.eq("HR"))
                .orderBy(employee.firstName)
                .list(getEngine());
        assertEquals(Arrays.asList("Bill", "Margaret"), list);
    }

    private AbstractQueryBaseScalar<String> distinctFirstNames(final Employee employee) {
        return employee.firstName.distinct();
    }

    private AbstractQueryBaseScalar<String> allFirstNames(final Employee employee) {
        return employee.firstName.selectAll();
    }

    public void testWrongOrderBy() throws Exception {
        // order by a column, which does not appear in DISTINCT select list, is ambiguous:
        // which James should we keep - Cooper (1st) of First(2nd)?
        // but MySQL does NOT throw exception
        final Employee employee = new Employee();
        try {
            final List<String> list = distinctFirstNames(employee).orderBy(employee.lastName).list(getEngine());
            assertEquals(Arrays.asList("James", "Bill", "Alex", "Margaret"), list);
        } catch (SQLException e) {
            // derby: ERROR 42879: The ORDER BY clause may not contain column 'LAST_NAME', since the query specifies DISTINCT and that column does not appear in the query result
            // org.postgresql.util.PSQLException: ERROR: for SELECT DISTINCT, ORDER BY expressions must appear in select list
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

}
