package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.front.StatementOptions;
import org.simqle.generic.Functions;
import org.simqle.integration.model.BigTable;
import org.simqle.integration.model.Country;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.integration.model.MyDual;
import org.simqle.sql.AbstractSelectList;

import javax.sql.DataSource;
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
        usaCountryId = country.countryId.where(country.code.eq("USA")).list(getDialectDataSource()).get(0);
        final Department dept = new Department();
        devDeptId = dept.deptId.where(dept.deptName.eq("DEV")).list(getDialectDataSource()).get(0);
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> developers = employee.firstName.pair(employee.lastName).where(employee.department().deptName.eq("DEV")).list(getDialectDataSource());
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
                .where(employee.title.eq("guru")).list(getDialectDataSource());
        assertEquals(1, pairs.size());
        assertEquals(Pair.of("Pedersen", "First"), pairs.get(0));
    }

    public void testMultipleJoins() throws Exception {
        System.out.println(getName());
        final Employee employee = new Employee();
        final AbstractSelectList<Pair<String,String>> select = employee.department().country().code.pair(employee.department().manager().department().country().code);
        System.out.println(select.show());
        final List<Pair<String, String>> countryCodes = select.list(getDialectDataSource());
//        final Connection connection = getDialectDataSource().getDataSource().getConnection();
//        connection.prepareStatement("SELECT T3.code AS C1 FROM employee AS T1 LEFT JOIN  department AS T2 LEFT JOIN country AS T3 ON T3.country_id = T2.country_id ON T2.dept_id = T1.dept_id");

        assertEquals(5, countryCodes.size());
        System.out.println(countryCodes);
        for (Pair<String, String> pair : countryCodes) {
            assertEquals(pair.getFirst(), pair.getSecond());
        }
    }

    public void testSelect() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.list(getDialectDataSource());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    public void testSelectAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.all().list(getDialectDataSource());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    public void testSelectDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.distinct().list(getDialectDataSource());
        assertEquals(4, list.size());
        assertTrue(list.toString(), list.contains("Margaret"));
        assertTrue(list.toString(), list.contains("Bill"));
        assertTrue(list.toString(), list.contains("James"));
        assertTrue(list.toString(), list.contains("Alex"));
    }

    public void testAsFunctionArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = Functions.floor(employee.salary).list(getDialectDataSource());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains(3000.0));
        assertTrue(list.toString(), list.contains(2000.0));
        assertTrue(list.toString(), list.contains(1500.0));
    }

    public void testAsCondition() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.retired.booleanValue()).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ne(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.gt(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(Collections.emptyList(), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ge(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.lt(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.le(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "First", "Redwood")), new HashSet<String>(list));
    }

    public void testEqValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James")).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Cooper")), new HashSet<String>(list));
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ne("James")).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood")), new HashSet<String>(list));
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.gt("James")).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ge("James")).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Cooper", "Redwood")), new HashSet<String>(list));
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.lt("James")).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.le("James")).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "First", "Cooper")), new HashSet<String>(list));
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.exceptAll(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Cooper")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql does not support EXCEPT
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.except(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Cooper")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql does not support EXCEPT
            if (databaseIsNot("mysql")) {
                throw e;
            }
            // a workaround for EXCEPT - TODO
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName.exceptDistinct(department.manager().firstName.where(department.deptId.eq(1))).list(getDialectDataSource());
            assertEquals(3, list.size());
            assertTrue(list.toString(), list.contains("Bill"));
            assertTrue(list.toString(), list.contains("James"));
            assertTrue(list.toString(), list.contains("Alex"));
        } catch (SQLException e) {
            // mysql does not support EXCEPT
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.unionAll(department.manager().lastName).list(getDialectDataSource());
        assertEquals(7, list.size());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood", "First", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.unionDistinct(department.manager().lastName).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.union(department.manager().lastName).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersectAll(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql does not support EXCEPT
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersect(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql does not support EXCEPT
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.intersectDistinct(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("First", "Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // mysql does not support EXCEPT
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testSelectForUpdate() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.forUpdate().list(getDialectDataSource());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    public void testSelectForReadOnly() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.forReadOnly().list(getDialectDataSource());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    public void testExists() throws Exception {
        final Country country = new Country();
        final Department department = new Department();
        final List<String> list = country.code.where(department.deptId.exists()).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("RUS", "USA", "FRA")), new HashSet<String>(list));

    }

    public void testExistsWithCondition() throws Exception {
        final Country country = new Country();
        final Department department = new Department();
        final List<String> list = country.code.where(department.deptId.where(department.countryId.eq(country.countryId)).exists()).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("RUS", "USA")), new HashSet<String>(list));

    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.in(department.manager().firstName)).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);

    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(employee.firstName.notIn(department.manager().firstName)).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.in("James", "Bill")).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Cooper"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.notIn("James", "Bill")).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "Pedersen"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.deptId.isNull()).list(getDialectDataSource());
        assertEquals(Collections.singletonList("Cooper"), list);
    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.deptId.isNotNull()).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        assertEquals(expected, list);
    }

    public void testOrderByTwoColumns() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.firstName, employee.lastName).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Pedersen", "March", "Cooper", "First", "Redwood"));
        assertEquals(expected, list);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(employee.deptId.nullsFirst()).list(getDialectDataSource());
            assertEquals("Cooper", list.get(0));
        } catch (SQLException e) {
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testOrderByNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(employee.deptId.nullsLast()).list(getDialectDataSource());
            assertEquals("Cooper", list.get(list.size()-1));
        } catch (SQLException e) {
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testOrderByAsc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName.asc()).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        assertEquals(expected, list);
    }

    public void testOrderByDesc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName.desc()).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        Collections.reverse(expected);
        assertEquals(expected, list);
    }

    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.opposite().where(employee.lastName.eq("Redwood")).list(getDialectDataSource());
        assertEquals(Arrays.asList(-3000.0), list);
    }

    public void testPlus() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.empId.plus(employee.deptId).list(getDialectDataSource());
        final List<Pair<Integer, Integer>> pairs = employee.empId.pair(employee.deptId).list(getDialectDataSource());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : pairs) {
            expected.add(pair.getFirst() == null || pair.getSecond() == null ? null : pair.getFirst() + pair.getSecond());
        }
        assertEquals(expected, actual);
    }

    public void testMinus() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.empId.minus(employee.deptId).list(getDialectDataSource());
        final List<Pair<Integer, Integer>> pairs = employee.empId.pair(employee.deptId).list(getDialectDataSource());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : pairs) {
            expected.add(pair.getFirst() == null || pair.getSecond() == null ? null : pair.getFirst() - pair.getSecond());
        }
        assertEquals(expected, actual);
    }

    public void testMult() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.empId.mult(employee.deptId).list(getDialectDataSource());
        final List<Pair<Integer, Integer>> pairs = employee.empId.pair(employee.deptId).list(getDialectDataSource());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : pairs) {
            expected.add(pair.getFirst() == null || pair.getSecond() == null ? null : pair.getFirst() * pair.getSecond());
        }
        assertEquals(expected, actual);
    }

    public void testDiv() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.empId.div(employee.deptId).list(getDialectDataSource());
        final List<Pair<Integer, Integer>> pairs = employee.empId.pair(employee.deptId).list(getDialectDataSource());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : pairs) {
            expected.add(pair.getFirst() == null || pair.getSecond() == null ? null : pair.getFirst() / pair.getSecond());
        }
        assertEquals(expected, actual);
    }

    public void testPlusNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> sums = employee.empId.plus(1).list(getDialectDataSource());
        final List<Integer> list = employee.empId.list(getDialectDataSource());
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

    public void testMinusNumber() throws Exception {
        final Employee employee = new Employee();
        final List<Number> sums = employee.empId.minus(1).list(getDialectDataSource());
        final List<Integer> list = employee.empId.list(getDialectDataSource());
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
        final List<Number> sums = employee.empId.mult(2).list(getDialectDataSource());
        final List<Integer> list = employee.empId.list(getDialectDataSource());
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
        final List<Number> sums = employee.empId.div(2).list(getDialectDataSource());
        final List<Integer> list = employee.empId.list(getDialectDataSource());
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
        try {
            final List<String> list = employee.firstName.concat(employee.lastName).where(employee.lastName.eq("Redwood")).list(getDialectDataSource());
            assertEquals(Arrays.asList("MargaretRedwood"), list);
        } catch (IllegalStateException e) {
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testConcatString() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.firstName.concat(" expected").where(employee.lastName.eq("Redwood")).list(getDialectDataSource());
            assertEquals(Arrays.asList("Margaret expected"), list);
        } catch (IllegalStateException e) {
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testCount() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.count().list(getDialectDataSource());
        assertEquals(Arrays.asList(5), list);

    }

    public void testCountDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.firstName.countDistinct().list(getDialectDataSource());
        assertEquals(Arrays.asList(4), list);

    }

    public void testAvg() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.avg().list(getDialectDataSource());
        assertEquals(1, list.size());
        assertEquals(2300, list.get(0).intValue());
    }

    public void testSum() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.sum().list(getDialectDataSource());
        assertEquals(1, list.size());
        assertEquals(11500, list.get(0).intValue());
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.min().list(getDialectDataSource());
        assertEquals(1, list.size());
        assertEquals(1500, list.get(0).intValue());
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.max().list(getDialectDataSource());
        assertEquals(1, list.size());
        assertEquals(3000, list.get(0).intValue());
    }

    public void testQueryValue() throws Exception {
        MyDual dual = new MyDual();
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = dual.dummy.queryValue().pair(employee.lastName)
                .orderBy(employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.of("X", "Cooper"), Pair.of("X", "First"), Pair.of("X", "March"), Pair.of("X", "Pedersen"), Pair.of("X", "Redwood")), list);
    }

    public void testLike() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.like("%es")).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("First", "Cooper"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);

    }

    public void testNotLike() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.notLike("%es")).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testMaxRows() throws Exception {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.orderBy(employee.lastName).list(getDialectDataSource(), StatementOptions.setMaxRows(2));
            assertEquals(2, list.size());
            assertTrue(list.toString(), list.contains("First"));
            assertTrue(list.toString(), list.contains("Cooper"));
    }

    public void testFetchSize() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.list(getDialectDataSource(), StatementOptions.setFetchSize(2));
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    public void testFetchDirection() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName).list(getDialectDataSource(), StatementOptions.setFetchDirection(ResultSet.FETCH_REVERSE));
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testMaxFieldSize() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(employee.lastName).list(getDialectDataSource(), StatementOptions.setMaxFieldSize(5));
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Peder", "Redwo", "Coope"));
        final ArrayList<String> expectedForMysql = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        Collections.sort(expectedForMysql);
        Collections.sort(list);
        assertEquals(databaseIsNot("mysql") ? expected : expectedForMysql, list);
    }

    public void testQueryTimeout() throws Exception {
        final DataSource dataSource = getDialectDataSource().getDataSource();
        final Connection connection = dataSource.getConnection();
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
            final List<Integer> list = bigTable.num.list(getDialectDataSource(), StatementOptions.setQueryTimeout(1));
            if (databaseIsNot("derby")) {
                fail("No timeout in " + (System.currentTimeMillis() - start) + " millis");
            }
        } catch (SQLException e) {
            // fine
        }
    }

}
