package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.generic.Functions;
import org.simqle.integration.model.Country;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.sql.AbstractSelectList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

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
        assertEquals(new HashSet<String>(Arrays.asList("First", "Cooper", "Redwood")), new HashSet<String>(list));
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
        assertEquals(new HashSet<String>(Arrays.asList("First", "Cooper", "Redwood")), new HashSet<String>(list));
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.lt(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen")), new HashSet<String>(list));
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.le(employee.department().manager().firstName)).list(getDialectDataSource());
        assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood")), new HashSet<String>(list));
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
            assertEquals("mysql", getDatabaseName());
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
            assertEquals("mysql", getDatabaseName());
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
            assertEquals("mysql", getDatabaseName());
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
            assertEquals("mysql", getDatabaseName());
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
            assertEquals("mysql", getDatabaseName());
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
            assertEquals("mysql", getDatabaseName());
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
}
