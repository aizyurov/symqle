package org.symqle.integration;

import org.symqle.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.gate.MySqlDialect;
import org.symqle.sql.AbstractSelectList;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class PairTest extends AbstractIntegrationTestBase {

    public PairTest() throws Exception {
    }

    private AbstractSelectList<Pair<Double, String>> makePair(final Employee employee) {
        return employee.salary.pair(employee.firstName);
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).list(getDatabaseGate());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(Pair.make(1500.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "Margaret")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Bill")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Alex")));
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final AbstractSelectList<Pair<Double, String>> pair = makePair(employee);
        final List<Pair<Pair<Double, String>, String>> list = pair.pair(employee.department().deptName).where(employee.lastName.eq("Redwood")).list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(Pair.make(Pair.make(3000.0, "Margaret"), "HR"), list.get(0));
    }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final AbstractSelectList<Pair<Double, String>> pair = makePair(employee);
        final List<Pair<Double, String>> list = pair.where(employee.lastName.eq("Redwood")).list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(Pair.make(3000.0, "Margaret"), list.get(0));

    }

    public void testSelectAll() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).all().list(getDatabaseGate());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(Pair.make(1500.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "Margaret")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Bill")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Alex")));
    }

    public void testSelectDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).distinct().list(getDatabaseGate());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(Pair.make(1500.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "Margaret")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Bill")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Alex")));
    }

    public void testSelectForUpdate() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).forUpdate().list(getDatabaseGate());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(Pair.make(1500.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "Margaret")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Bill")));
        assertTrue(list.toString(), list.contains(Pair.make(3000.0, "James")));
        assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Alex")));
    }

    public void testSelectForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Pair<Double, String>> list = makePair(employee).forReadOnly().list(getDatabaseGate());
            assertEquals(5, list.size());
            assertTrue(list.toString(), list.contains(Pair.make(1500.0, "James")));
            assertTrue(list.toString(), list.contains(Pair.make(3000.0, "Margaret")));
            assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Bill")));
            assertTrue(list.toString(), list.contains(Pair.make(3000.0, "James")));
            assertTrue(list.toString(), list.contains(Pair.make(2000.0, "Alex")));
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

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).orderBy(employee.lastName).list(getDatabaseGate());
        final List<Pair<Double, String>> expected = Arrays.asList(
                Pair.make(1500.0, "James"),
                Pair.make(3000.0, "James"),
                Pair.make(2000.0, "Bill"),
                Pair.make(2000.0, "Alex"),
                Pair.make(3000.0, "Margaret"));
        assertEquals(expected, list);
    }

    public void testOrderByAsc() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double, String>> list = makePair(employee).orderBy(employee.lastName.asc()).list(getDatabaseGate());
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
        final List<Pair<Double, String>> list = makePair(employee).orderBy(employee.lastName.desc()).list(getDatabaseGate());
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
