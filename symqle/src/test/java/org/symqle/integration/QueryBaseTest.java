package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractQueryBase;
import org.symqle.testset.AbstractQueryBaseTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class QueryBaseTest extends AbstractIntegrationTestBase implements AbstractQueryBaseTestSet {

    public QueryBaseTest() throws Exception {
    }

    private AbstractQueryBase<Pair<Double, String>> makePair(final Employee employee) {
        return employee.salary.pair(employee.firstName).selectAll();
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).compileQuery(getEngine()).list();
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(Pair.make(1500.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "Margaret")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Bill")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Alex")));
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).forReadOnly().list(getEngine());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(Pair.make(1500.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "Margaret")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Bill")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Alex")));
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).forUpdate().list(getEngine());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(Pair.make(1500.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "Margaret")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Bill")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Alex")));
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).limit(2, 5).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).list(getEngine());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(Pair.make(1500.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "Margaret")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Bill")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Alex")));
    }

    @Override
    public void test_countRows_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = makePair(employee).countRows().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).orderBy(employee.lastName).list(getEngine());
        final List<Pair<Double, String>> expected = Arrays.asList(
                Pair.make(1500.0, "James"),
                Pair.make(3000.0, "James"),
                Pair.make(2000.0, "Bill"),
                Pair.make(2000.0, "Alex"),
                Pair.make(3000.0, "Margaret"));
        assertEquals(expected, list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> expected = new ArrayList<>(Arrays.asList(
                Pair.make(1500.0, "James"),
                Pair.make(3000.0, "James"),
                Pair.make(2000.0, "Bill"),
                Pair.make(2000.0, "Alex"),
                Pair.make(3000.0, "Margaret")));
        final int count = makePair(employee).scroll(getEngine(), new Callback<Pair<Double, String>>() {
            @Override
            public boolean iterate(final Pair<Double, String> doubleStringPair) throws SQLException {
                assertTrue(expected + " does not contain " + doubleStringPair, expected.remove(doubleStringPair));
                return true;
            }
        });
        assertEquals(5, count);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = makePair(employee).showQuery(getEngine().getDialect());
        assertSimilar("SELECT ALL T0.salary AS C0, T0.first_name AS C1 FROM employee AS T0", sql);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make(3000.0, "Margaret"), list.get(0));
    }

    public void testOrderByAsc() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).orderBy(employee.lastName.asc()).list(getEngine());
        final List<Pair<Double, String>> expected = Arrays.asList(
                Pair.make(1500.0, "James"),
                Pair.make(3000.0, "James"),
                Pair.make(2000.0, "Bill"),
                Pair.make(2000.0, "Alex"),
                Pair.make(3000.0, "Margaret"));
        assertEquals(expected, list);
    }

    public void testOrderByDesc() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).orderBy(employee.lastName.desc()).list(getEngine());
        final List<Pair<Double, String>> expected = Arrays.asList(
                Pair.make(1500.0, "James"),
                Pair.make(3000.0, "James"),
                Pair.make(2000.0, "Bill"),
                Pair.make(2000.0, "Alex"),
                Pair.make(3000.0, "Margaret"));
        Collections.reverse(expected);
        assertEquals(expected, list);
    }


}
