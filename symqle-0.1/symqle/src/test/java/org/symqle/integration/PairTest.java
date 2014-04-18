package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractSelectList;
import org.symqle.testset.AbstractSelectListTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class PairTest extends AbstractIntegrationTestBase implements AbstractSelectListTestSet {

    public PairTest() throws Exception {
    }

    private AbstractSelectList<Pair<Double, String>> makePair(final Employee employee) {
        return employee.salary.pair(employee.firstName);
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
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).distinct().list(getEngine());
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
        final List<Pair<Double, String>> list = makePair(employee).limit(2, 10).list(getEngine());
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
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final AbstractSelectList<Pair<Double, String>> pair = makePair(employee);
        final List<Pair<Pair<Double, String>, String>> list = pair.pair(employee.department().deptName).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make(Pair.make(3000.0, "Margaret"), "HR"), list.get(0));
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSelectList<Pair<Double, String>> pair = makePair(employee);
        final List<Pair<String, Pair<Double, String>>> list = employee.department().deptName.pair(pair).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("HR", Pair.make(3000.0, "Margaret")), list.get(0));
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
        makePair(employee).orderBy(employee.lastName).scroll(getEngine(), new Callback<Pair<Double, String>>() {
            @Override
            public boolean iterate(final Pair<Double, String> pair) throws SQLException {
                assertTrue(expected.toString(), expected.remove(pair));
                return true;
            }
        });
        assertTrue(expected.toString(), expected.isEmpty());

    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).selectAll().list(getEngine());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(Pair.make(1500.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "Margaret")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Bill")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Alex")));
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).list(getEngine());
        Pattern expected = Pattern.compile("SELECT ([A-Z][A-Z0-9]*).salary AS [A-Z][A-Z0-9]*, \\1\\.last_name AS [A-Z][A-Z0-9]* FROM employee AS \\1");

    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final AbstractSelectList<Pair<Double, String>> pair = makePair(employee);
        final List<Pair<Double, String>> list = pair.where(employee.lastName.eq("Redwood")).list(getEngine());
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
