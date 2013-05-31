package org.simqle.integration;

import org.simqle.Mappers;
import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.generic.Functions;
import org.simqle.integration.model.Country;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.sql.DynamicParameter;

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
public class DynamicParameterTest extends AbstractIntegrationTestBase {

    private Integer usaCountryId;
    private Integer devDeptId;

    public DynamicParameterTest() throws Exception {
    }

    @Override
    public void onSetUp() throws Exception {
        final Country country = new Country();
        usaCountryId = country.countryId.where(country.code.eq("USA")).list(getDialectDataSource()).get(0);
        final Department dept = new Department();
        devDeptId = dept.deptId.where(dept.deptName.eq("DEV")).list(getDialectDataSource()).get(0);
    }

    public void testSelect() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(Mappers.INTEGER, 1).list(getDialectDataSource());
            assertEquals(Arrays.asList(1), list);
        } catch (IllegalStateException e) {
            if (!databaseIsNot("mysql")) {
                fail("SELECT with no FROM must be supported by this engine");
            }
        }
    }

    public void testSelectAll() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(Mappers.INTEGER, 1).all().list(getDialectDataSource());
            assertEquals(Arrays.asList(1), list);
        } catch (IllegalStateException e) {
            if (!databaseIsNot("mysql")) {
                fail("SELECT with no FROM must be supported by this engine");
            }
        }
    }

    public void testSelectDistinct() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(Mappers.INTEGER, 1).all().list(getDialectDataSource());
            assertEquals(Arrays.asList(1), list);
        } catch (IllegalStateException e) {
            if (!databaseIsNot("mysql")) {
                fail("SELECT with no FROM must be supported by this engine");
            }
        }
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Pair<Integer, String>> redwood = Params.p(1).pair(employee.firstName).where(employee.lastName.eq("Redwood")).list(getDialectDataSource());
            assertEquals(Arrays.asList(Pair.of(1, "Margaret")), redwood);
        } catch (SQLException e) {
            if (databaseIsNot("embeddedDerby")) {
                fail("This engine should support parameters in select list");
            }
        }
    }

    public void testAsFunctionArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Double>> list = employee.lastName.pair(Functions.floor(Params.p(1.2))).where(employee.lastName.eq("Redwood")).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.of("Redwood", 1.0)), list);
    }

    public void testAsCondition() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p(false).booleanValue()).list(getDialectDataSource());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            if (databaseIsNot("embeddedDerby")) {
                throw e;
            }
        }
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("Margaret").eq(employee.firstName)).list(getDialectDataSource());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("Margaret").ne(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Pedersen")), new HashSet<String>(list));
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("K").gt(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Pedersen")), new HashSet<String>(list));
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("James").ge(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("First", "Pedersen")), new HashSet<String>(list));
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("K").lt(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Redwood")), new HashSet<String>(list));
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("Margaret").le(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Redwood")), new HashSet<String>(list));
    }

    public void testEqValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").eq("James")).list(getDialectDataSource());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // ERROR 42X35: It is not allowed for both operands of '<>' to be ? parameters.
            if (databaseIsNot("embeddedDerby")) {
                throw e;
            }
        }
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").ne("James")).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood", "Cooper", "First")), new HashSet<String>(list));
        } catch (SQLException e) {
            if (databaseIsNot("embeddedDerby")) {
                throw e;
            }
        }
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").gt("James")).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood", "Cooper", "First")), new HashSet<String>(list));
        } catch (SQLException e) {
            if (databaseIsNot("embeddedDerby")) {
                throw e;
            }
        }
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").ge("James")).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood", "Cooper", "First")), new HashSet<String>(list));
        } catch (SQLException e) {
            if (databaseIsNot("embeddedDerby")) {
                throw e;
            }
        }
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").lt("James")).list(getDialectDataSource());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            if (databaseIsNot("embeddedDerby")) {
                throw e;
            }
        }
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("James").le("Margaret")).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood", "Cooper", "First")), new HashSet<String>(list));
        } catch (SQLException e) {
            if (databaseIsNot("embeddedDerby")) {
                throw e;
            }
        }
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").exceptAll(employee.firstName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (IllegalStateException e) {
            // most databases do not support SELECt without FROM
            if (!databaseIsNot("mysql")) {
                throw e;
            }
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
            final List<String> list = Params.p("Redwood").except(employee.firstName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (IllegalStateException e) {
            // most databases do not support SELECt without FROM
            if (!databaseIsNot("mysql")) {
                throw e;
            }
        } catch (SQLException e) {
            // mysql does not support EXCEPT
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").exceptDistinct(employee.firstName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (IllegalStateException e) {
            // most databases do not support SELECt without FROM
            if (!databaseIsNot("mysql")) {
                throw e;
            }
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
        try {
            final List<String> list = Params.p("Redwood").unionAll(employee.lastName).list(getDialectDataSource());
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "Redwood", "First", "Cooper", "March", "Pedersen"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (IllegalStateException e) {
            // most databases do not support SELECt without FROM
            if (!databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").unionDistinct(employee.lastName).list(getDialectDataSource());
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "First", "Cooper", "March", "Pedersen"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (IllegalStateException e) {
            // most databases do not support SELECt without FROM
            if (!databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").union(employee.lastName).list(getDialectDataSource());
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "First", "Cooper", "March", "Pedersen"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (IllegalStateException e) {
            // most databases do not support SELECt without FROM
            if (!databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").intersectAll(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (IllegalStateException e) {
            // most databases do not support SELECT without FROM
            if (!databaseIsNot("mysql")) {
                throw e;
            }
        } catch (SQLException e) {
            // mysql does not support INTERSECT
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").intersect(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (IllegalStateException e) {
            // most databases do not support SELECT without FROM
            if (!databaseIsNot("mysql")) {
                throw e;
            }
        } catch (SQLException e) {
            // mysql does not support INTERSECT
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").intersectDistinct(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (IllegalStateException e) {
            // most databases do not support SELECT without FROM
            if (!databaseIsNot("mysql")) {
                throw e;
            }
        } catch (SQLException e) {
            // mysql does not support INTERSECT
            if (databaseIsNot("mysql")) {
                throw e;
            }
        }
    }

    public void testSelectForUpdate() throws Exception {
        if (databaseIsNot("mysql")) {
            return;
        }
        final List<String> list = Params.p("Redwood").forUpdate().list(getDialectDataSource());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    public void testSelectForReadOnly() throws Exception {
        if (databaseIsNot("mysql")) {
            return;
        }
        final List<String> list = Params.p("Redwood").forUpdate().list(getDialectDataSource());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    public void testExists() throws Exception {
        if (databaseIsNot("mysql")) {
            return;
        }
        final Country country = new Country();
        final List<String> list = country.code.where(Params.p("Redwood").exists()).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("RUS", "USA", "FRA")), new HashSet<String>(list));

    }

    public void testExistsWithCondition() throws Exception {
        if (databaseIsNot("mysql")) {
            return;
        }
        final Country country = new Country();
        final Department department = new Department();
        final List<String> list = country.code.where(Params.p("Redwood").where(department.countryId.eq(country.countryId)).exists()).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("RUS", "USA")), new HashSet<String>(list));

    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        System.out.println(employee.lastName.where(Params.p("Margaret").in(department.manager().firstName.where(employee.department().deptId.eq(department.deptId)))).show(getDialectDataSource().getDialect()));
        final List<String> list = employee.lastName.where(Params.p("Margaret").in(department.manager().firstName.where(employee.department().deptId.eq(department.deptId)))).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "March"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);

    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.where(Params.p("James").notIn(department.manager().firstName.where(employee.department().deptId.eq(department.deptId)))).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Cooper", "Redwood", "March"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("James").in("Bill", "James")).list(getDialectDataSource());
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Pedersen", "March", "Cooper", "First", "Redwood"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            if (databaseIsNot("embeddedDerby")) {
                throw e;
            }
        }
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("James").notIn("Bill", "Alex")).list(getDialectDataSource());
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Pedersen", "March", "Cooper", "First", "Redwood"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            if (databaseIsNot("embeddedDerby")) {
                throw e;
            }
        }
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
        final List<Double> list = employee.salary.sum().list(getDialectDataSource());
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
        final List<String> list = employee.lastName.where(Params.p("Bill").notLike("%es")).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "First", "Cooper", "March", "Pedersen"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }


}
