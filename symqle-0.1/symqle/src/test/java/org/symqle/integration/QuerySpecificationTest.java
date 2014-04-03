package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractQuerySpecification;
import org.symqle.testset.AbstractQuerySpecificationTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class QuerySpecificationTest extends AbstractIntegrationTestBase implements AbstractQuerySpecificationTestSet {

    private AbstractQuerySpecification<Pair<String, String>> querySpec(final Employee employee) {
        return employee.firstName.pair(employee.lastName).where(employee.salary.gt(2500.0));
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = querySpec(employee).compileQuery(getEngine()).list();
        Collections.sort(list, new Comparator<Pair<String, String>>() {
            @Override
            public int compare(final Pair<String, String> o1, final Pair<String, String> o2) {
                final int lastNameComparison = o1.second().compareTo(o2.second());
                return lastNameComparison == 0 ? o1.first().compareTo(o2.first()) : lastNameComparison;
            }
        });
        assertEquals(Arrays.asList(Pair.make("James", "First"), Pair.make("Margaret", "Redwood")), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = querySpec(employee).forReadOnly().list(getEngine());
        Collections.sort(list, new Comparator<Pair<String, String>>() {
            @Override
            public int compare(final Pair<String, String> o1, final Pair<String, String> o2) {
                final int lastNameComparison = o1.second().compareTo(o2.second());
                return lastNameComparison == 0 ? o1.first().compareTo(o2.first()) : lastNameComparison;
            }
        });
        assertEquals(Arrays.asList(Pair.make("James", "First"), Pair.make("Margaret", "Redwood")), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = querySpec(employee).forUpdate().list(getEngine());
        Collections.sort(list, new Comparator<Pair<String, String>>() {
            @Override
            public int compare(final Pair<String, String> o1, final Pair<String, String> o2) {
                final int lastNameComparison = o1.second().compareTo(o2.second());
                return lastNameComparison == 0 ? o1.first().compareTo(o2.first()) : lastNameComparison;
            }
        });
        assertEquals(Arrays.asList(Pair.make("James", "First"), Pair.make("Margaret", "Redwood")), list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = querySpec(employee).limit(1).list(getEngine());
        assertEquals(1, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = querySpec(employee).limit(1, 5).list(getEngine());
        assertEquals(1, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = querySpec(employee).list(getEngine());
        Collections.sort(list, new Comparator<Pair<String, String>>() {
            @Override
            public int compare(final Pair<String, String> o1, final Pair<String, String> o2) {
                final int lastNameComparison = o1.second().compareTo(o2.second());
                return lastNameComparison == 0 ? o1.first().compareTo(o2.first()) : lastNameComparison;
            }
        });
        assertEquals(Arrays.asList(Pair.make("James", "First"), Pair.make("Margaret", "Redwood")), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = querySpec(employee).orderBy(employee.lastName).forReadOnly().list(getEngine());
        assertEquals(Arrays.asList(Pair.make("James", "First"), Pair.make("Margaret", "Redwood")), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> expected = new ArrayList<>(Arrays.asList(Pair.make("James", "First"), Pair.make("Margaret", "Redwood")));
        final int count = querySpec(employee).scroll(getEngine(), new Callback<Pair<String, String>>() {
            @Override
            public boolean iterate(final Pair<String, String> pair) throws SQLException {
                assertTrue(expected + " does not contain " + pair, expected.remove(pair));
                return true;
            }
        });
        assertEquals(2, count);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = querySpec(employee).showQuery(getEngine().getDialect());
        final Pattern expected;
        if (getDatabaseName().equals("PostgreSQL")) {
            expected = Pattern.compile("SELECT ([A-Z][A-Z0-9]*)\\.first_name AS [A-Z][A-Z0-9]*, \\1\\.last_name AS [A-Z][A-Z0-9]* FROM employee AS \\1 WHERE\\(\\1\\.salary > \\?\\)");
        } else {
            expected = Pattern.compile("SELECT ([A-Z][A-Z0-9]*)\\.first_name AS [A-Z][A-Z0-9]*, \\1\\.last_name AS [A-Z][A-Z0-9]* FROM employee AS \\1 WHERE \\1\\.salary > \\?");
        }
        assertTrue(sql, expected.matcher(sql).matches());
    }


}
