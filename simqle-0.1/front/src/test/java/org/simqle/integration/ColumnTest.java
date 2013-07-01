package org.simqle.integration;

import junit.framework.AssertionFailedError;
import org.simqle.Mappers;
import org.simqle.Pair;
import org.simqle.generic.Functions;
import org.simqle.integration.model.BigTable;
import org.simqle.integration.model.Country;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.integration.model.MyDual;
import org.simqle.jdbc.Option;
import org.simqle.mysql.MySqlDialect;
import org.simqle.sql.AbstractSelectList;
import org.simqle.sql.Dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author lvovich
 */
public class ColumnTest extends AbstractIntegrationTestBase {

    private Integer usaCountryId;
    private Integer devDeptId;

    public ColumnTest() throws Exception {
    }

    @Override
    public void onSetUp() throws Exception {
        final Country country = new Country();
        usaCountryId = country.countryId.where(country.code.eq("USA")).list(getDatabaseGate()).get(0);
        final Department dept = new Department();
        devDeptId = dept.deptId.where(dept.deptName.eq("DEV")).list(getDatabaseGate()).get(0);
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> developers = employee.firstName.pair(employee.lastName).where(employee.department().deptName.eq("DEV")).list(getDatabaseGate());
        System.out.println(developers);
    }

    public void testSubqueryInSelect() throws Exception {
        final Employee employee = new Employee();
        final Employee manager = new Employee();
        final List<Pair<String, String>> pairs = employee.lastName
                .pair(
                        manager.lastName.where(employee.department().deptId.eq(manager.department().deptId)
                                .and(manager.title.like("%manager"))).queryValue()
                )
                .where(employee.title.eq("guru")).list(getDatabaseGate());
        assertEquals(1, pairs.size());
        assertEquals(Pair.make("Pedersen", "First"), pairs.get(0));
    }

    public void testMultipleJoins() throws Exception {
        System.out.println(getName());
        final Employee employee = new Employee();
        final AbstractSelectList<Pair<String,String>> select = employee.department().country().code.pair(employee.department().manager().department().country().code);
        System.out.println(select.show());
        final List<Pair<String, String>> countryCodes = select.list(getDatabaseGate());
//        final Connection connection = getGate().getDataSource().getConnection();
//        connection.prepareStatement("SELECT T3.code AS C1 FROM employee AS T1 LEFT JOIN  department AS T2 LEFT JOIN country AS T3 ON T3.country_id = T2.country_id ON T2.dept_id = T1.dept_id");

        assertEquals(5, countryCodes.size());
        System.out.println(countryCodes);
        for (Pair<String, String> pair : countryCodes) {
            assertEquals(pair.first(), pair.second());
        }
    }

    public void testSelect() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.list(getDatabaseGate());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    public void testCast() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.cast("CHAR(8)").list(getDatabaseGate());
        Collections.sort(list);
        try {
            assertEquals(Arrays.asList("Cooper  ", "First   ", "March   ", "Pedersen", "Redwood "), list);
        } catch (AssertionFailedError e) {
            if ("mysql".equals(getDatabaseName())) {
                // mysql trheats CHAR as VARCHAR, blanks are not appended
                assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
            } else {
                throw e;
            }
        }
    }

    public void testMap() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.salary.map(Mappers.INTEGER).orderBy(employee.lastName).list(getDatabaseGate());
        assertEquals(Arrays.asList(1500, 3000, 2000, 2000, 3000), list);
    }

    public void testSelectAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.all().list(getDatabaseGate());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    public void testSelectDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.distinct().list(getDatabaseGate());
        assertEquals(4, list.size());
        assertTrue(list.toString(), list.contains("Margaret"));
        assertTrue(list.toString(), list.contains("Bill"));
        assertTrue(list.toString(), list.contains("James"));
        assertTrue(list.toString(), list.contains("Alex"));
    }

    public void testAsFunctionArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = Functions.floor(employee.salary).list(getDatabaseGate());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(3000.0));
        assertTrue(list.toString(), list.contains(2000.0));
        assertTrue(list.toString(), list.contains(1500.0));
    }

    public void testAsCondition() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.retired.booleanValue()).list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq(employee.department().manager().firstName)).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ne(employee.department().manager().firstName)).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.gt(employee.department().manager().firstName)).list(getDatabaseGate());
        assertEquals(Collections.emptyList(), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ge(employee.department().manager().firstName)).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.lt(employee.department().manager().firstName)).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.le(employee.department().manager().firstName)).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "First", "Redwood")), new HashSet<String>(list));
    }

    public void testEqValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James")).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Cooper")), new HashSet<String>(list));
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ne("James")).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood")), new HashSet<String>(list));
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.gt("James")).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ge("James")).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Cooper", "Redwood")), new HashSet<String>(list));
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.lt("James")).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.le("James")).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "First", "Cooper")), new HashSet<String>(list));
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.exceptAll(department.manager().lastName).list(getDatabaseGate());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Cooper")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.except(department.manager().lastName).list(getDatabaseGate());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Cooper")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName.exceptDistinct(department.manager().firstName.where(department.deptId.eq(1))).list(getDatabaseGate());
            assertEquals(3, list.size());
            assertTrue(list.toString(), list.contains("Bill"));
            assertTrue(list.toString(), list.contains("James"));
            assertTrue(list.toString(), list.contains("Alex"));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.unionAll(department.manager().lastName).list(getDatabaseGate());
        assertEquals(7, list.size());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood", "First", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.unionDistinct(department.manager().lastName).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.union(department.manager().lastName).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersectAll(department.manager().lastName).list(getDatabaseGate());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersect(department.manager().lastName).list(getDatabaseGate());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersectDistinct(department.manager().lastName).list(getDatabaseGate());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testSelectForUpdate() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.forUpdate().list(getDatabaseGate());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    public void testSelectForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.forReadOnly().list(getDatabaseGate());
            assertEquals(5, list.size());
            assertTrue(list.toString(), list.contains("Cooper"));
            assertTrue(list.toString(), list.contains("Redwood"));
            assertTrue(list.toString(), list.contains("March"));
            assertTrue(list.toString(), list.contains("First"));
            assertTrue(list.toString(), list.contains("Pedersen"));
        } catch (SQLException e) {
            if (MySqlDialect.class.equals(getDatabaseGate().getDialect().getClass())) {
                // should work with MySqlDialect
                throw e;
            } else {
                // mysql does not support FOR READ ONLY natively
                expectSQLException(e, "mysql");
            }
        }
    }

    public void testExists() throws Exception {
        final Country country = new Country();
        final Department department = new Department();
        final List<String> list = country.code.where(department.deptId.exists()).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("RUS", "USA", "FRA")), new HashSet<String>(list));
    }

    public void testContains() throws Exception {
        final Country country = new Country();
        final Department department = new Department();
        final List<String> list = country.code.where(department.deptId.contains(-1)).list(getDatabaseGate());
        assertEquals(0, list.size());
    }

    public void testExistsWithCondition() throws Exception {
        final Country country = new Country();
        final Department department = new Department();
        final List<String> list = country.code.where(department.deptId.where(department.countryId.eq(country.countryId)).exists()).list(getDatabaseGate());
        assertEquals(new HashSet<String>(Arrays.asList("RUS", "USA")), new HashSet<String>(list));

    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.in(department.manager().firstName)).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);

    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.notIn(department.manager().firstName)).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.in("James", "Bill")).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Cooper"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.notIn("James", "Bill")).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "Pedersen"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.deptId.isNull()).list(getDatabaseGate());
        assertEquals(Collections.singletonList("Cooper"), list);
    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.deptId.isNotNull()).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        assertEquals(expected, list);
    }

    public void testOrderByTwoColumns() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.firstName, employee.lastName).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Pedersen", "March", "Cooper", "First", "Redwood"));
        assertEquals(expected, list);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(employee.deptId.nullsFirst()).list(getDatabaseGate());
            assertEquals("Cooper", list.get(0));
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(employee.deptId.nullsLast()).list(getDatabaseGate());
            assertEquals("Cooper", list.get(list.size()-1));
        } catch (SQLException e) {
            final Class<? extends Dialect> dialectClass = getDatabaseGate().getDialect().getClass();
            // mysql does not support NULLS LAST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByAsc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName.asc()).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        assertEquals(expected, list);
    }

    public void testOrderByDesc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName.desc()).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        Collections.reverse(expected);
        assertEquals(expected, list);
    }

    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.opposite().where(employee.lastName.eq("Redwood")).list(getDatabaseGate());
        assertEquals(Arrays.asList(-3000.0), list);
    }

    public void testAdd() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.empId.add(employee.deptId).list(getDatabaseGate());
        final List<Pair<Integer, Integer>> pairs = employee.empId.pair(employee.deptId).list(getDatabaseGate());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : pairs) {
            expected.add(pair.first() == null || pair.second() == null ? null : pair.first() + pair.second());
        }
        assertEquals(expected, actual);
    }

    public void testSub() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.empId.sub(employee.deptId).list(getDatabaseGate());
        final List<Pair<Integer, Integer>> pairs = employee.empId.pair(employee.deptId).list(getDatabaseGate());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : pairs) {
            expected.add(pair.first() == null || pair.second() == null ? null : pair.first() - pair.second());
        }
        assertEquals(expected, actual);
    }

    public void testMult() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.empId.mult(employee.deptId).list(getDatabaseGate());
        final List<Pair<Integer, Integer>> pairs = employee.empId.pair(employee.deptId).list(getDatabaseGate());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : pairs) {
            expected.add(pair.first() == null || pair.second() == null ? null : pair.first() * pair.second());
        }
        assertEquals(expected, actual);
    }

    public void testDiv() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.empId.div(employee.deptId).list(getDatabaseGate());
        final List<Pair<Integer, Integer>> pairs = employee.empId.pair(employee.deptId).list(getDatabaseGate());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : pairs) {
            expected.add(pair.first() == null || pair.second() == null ? null : pair.first() / pair.second());
        }
        assertEquals(expected, actual);
    }

    public void testAddNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> sums = employee.empId.add(1).list(getDatabaseGate());
        final List<Integer> list = employee.empId.list(getDatabaseGate());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n : sums) {
            actual.add(n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Integer i : list) {
            expected.add(i + 1);
        }
        assertEquals(expected, actual);
    }

    public void testSubNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> sums = employee.empId.sub(1).list(getDatabaseGate());
        final List<Integer> list = employee.empId.list(getDatabaseGate());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n : sums) {
            actual.add(n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Integer i : list) {
            expected.add(i - 1);
        }
        assertEquals(expected, actual);
    }

    public void testMultNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> sums = employee.empId.mult(2).list(getDatabaseGate());
        final List<Integer> list = employee.empId.list(getDatabaseGate());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n : sums) {
            actual.add(n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Integer i : list) {
            expected.add(i * 2);
        }
        assertEquals(expected, actual);
    }

    public void testDivNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> sums = employee.empId.div(2).list(getDatabaseGate());
        final List<Integer> list = employee.empId.list(getDatabaseGate());
        System.out.println(list);
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n : sums) {
            actual.add(n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Integer i : list) {
            expected.add(i / 2);
        }
        assertEquals(expected, actual);
    }

    public void testConcat() throws Exception {
        final Employee employee = new Employee();
        final Class<? extends Dialect> dialectClass = getDatabaseGate().getDialect().getClass();
        final List<String> list = employee.firstName.concat(employee.lastName).where(employee.lastName.eq("Redwood")).list(getDatabaseGate());
        assertEquals(Arrays.asList("MargaretRedwood"), list);
    }

    public void testConcatString() throws Exception {
        final Employee employee = new Employee();
        final Class<? extends Dialect> dialectClass = getDatabaseGate().getDialect().getClass();
        final List<String> list = employee.firstName.concat(" expected").where(employee.lastName.eq("Redwood")).list(getDatabaseGate());
        assertEquals(Arrays.asList("Margaret expected"), list);
    }

    public void testCollate() throws Exception {
        final Employee employee = new Employee();
        final Class<? extends Dialect> dialectClass = getDatabaseGate().getDialect().getClass();
        try {
            final List<String> list = employee.firstName.collate("utf8_unicode_ci").where(employee.lastName.eq("Redwood")).list(getDatabaseGate());
            assertEquals(Arrays.asList("Margaret"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE" at line 1, column 63
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testCount() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.count().list(getDatabaseGate());
        assertEquals(Arrays.asList(5), list);

    }

    public void testCountDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.firstName.countDistinct().list(getDatabaseGate());
        assertEquals(Arrays.asList(4), list);

    }

    public void testAvg() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.avg().list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(2300, list.get(0).intValue());
    }

    public void testSum() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.sum().list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(11500, list.get(0).intValue());
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.min().list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(1500, list.get(0).intValue());
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.max().list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(3000, list.get(0).intValue());
    }

    public void testQueryValue() throws Exception {
        MyDual dual = new MyDual();
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = dual.dummy.queryValue().pair(employee.lastName)
                .orderBy(employee.lastName).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make("X", "Cooper"), Pair.make("X", "First"), Pair.make("X", "March"), Pair.make("X", "Pedersen"), Pair.make("X", "Redwood")), list);
    }

    public void testLike() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.like("%es")).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("First", "Cooper"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);

    }

    public void testNotLike() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.notLike("%es")).list(getDatabaseGate());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testMaxRows() throws Exception {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.orderBy(employee.lastName).list(getDatabaseGate(), Option.setMaxRows(2));
            assertEquals(2, list.size());
            assertTrue(list.toString(), list.contains("First"));
            assertTrue(list.toString(), list.contains("Cooper"));
    }

    public void testFetchSize() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.list(getDatabaseGate(), Option.setFetchSize(2));
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    public void testFetchDirection() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName).list(getDatabaseGate(), Option.setFetchDirection(ResultSet.FETCH_REVERSE));
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testMaxFieldSize() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName).list(getDatabaseGate(), Option.setMaxFieldSize(5));
        final ArrayList<String> truncatedNames = new ArrayList<String>(Arrays.asList("March", "First", "Peder", "Redwo", "Coope"));
        final ArrayList<String> fullNames = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(truncatedNames);
        Collections.sort(fullNames);
        Collections.sort(list);
        // some database engines to not honor setMaxFieldSize
        // mysql,...
        if ("mysql".equals(getDatabaseName())) {
            assertEquals(fullNames, list);
        } else {
            assertEquals(truncatedNames, list);
        }
    }

    public void testQueryTimeout() throws Exception {
        // derby: does not honor queryTimeout
        // skip this test
        if ("Apache Derby".equals(getDatabaseName())) {
            return;
        }
        final Connection connection = getDatabaseGate().getConnection();
        try {
            final PreparedStatement deleteStatement = connection.prepareStatement("delete from big_table");
            deleteStatement.executeUpdate();
            final PreparedStatement insertStatement = connection.prepareStatement("insert into big_table (num) values(?)");
            for (int i=0; i<2000000; i++) {
                insertStatement.setInt(1, i);
                assertEquals(1, insertStatement.executeUpdate());
            }
        } finally {
            connection.close();
        }
        final BigTable bigTable = new BigTable();
        final long start = System.currentTimeMillis();

        try {
            final List<Integer> list = bigTable.num.list(getDatabaseGate(), Option.setQueryTimeout(1));
            fail("No timeout in " + (System.currentTimeMillis() - start) + " millis");
        } catch (SQLException e) {
            System.out.println("Timeout in "+ (System.currentTimeMillis() - start) + " millis");
            // fine: timeout
        }
    }

}
