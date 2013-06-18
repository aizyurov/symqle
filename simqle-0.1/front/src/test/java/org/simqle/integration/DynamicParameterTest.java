package org.simqle.integration;

import org.simqle.Mappers;
import org.simqle.Pair;
import org.simqle.derby.DerbyDialect;
import org.simqle.front.Params;
import org.simqle.generic.Functions;
import org.simqle.integration.model.Country;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.sql.Dialect;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.GenericDialect;

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
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testSelectAll() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(Mappers.INTEGER, 1).all().list(getDialectDataSource());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testSelectDistinct() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(Mappers.INTEGER, 1).all().list(getDialectDataSource());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Pair<Integer, String>> redwood = Params.p(1).pair(employee.firstName).where(employee.lastName.eq("Redwood")).list(getDialectDataSource());
            assertEquals(Arrays.asList(Pair.make(1, "Margaret")), redwood);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        }
    }

    public void testAsFunctionArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Double>> list = employee.lastName.pair(Functions.floor(Params.p(1.2))).where(employee.lastName.eq("Redwood")).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make("Redwood", 1.0)), list);
    }

    public void testAsCondition() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p(false).booleanValue()).list(getDialectDataSource());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X19: The WHERE or HAVING clause or CHECK CONSTRAINT definition is an untyped parameter expression.  It must be a BOOLEAN expression.
            // DerbyDialect fixes this problem
            if (DerbyDialect.class.equals(getDialectDataSource().getDialect().getClass())) {
                // should not get here: be fixed by DerbyDialect!
                throw e;
            } else {
                expectSQLException(e, "derby");
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
            // derby: ERROR 42X35: It is not allowed for both operands of '=' to be ? parameters.
            expectSQLException(e, "derby");
        }
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").ne("James")).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood", "Cooper", "First")), new HashSet<String>(list));
        } catch (SQLException e) {
            final Class<? extends Dialect> dialectClass = getDialectDataSource().getDialect().getClass();
            // derby: ERROR 42X35: It is not allowed for both operands of '<>' to be ? parameters.
            expectSQLException(e, "derby");
        }
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").gt("James")).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood", "Cooper", "First")), new HashSet<String>(list));
        } catch (SQLException e) {
            final Class<? extends Dialect> dialectClass = getDialectDataSource().getDialect().getClass();
            // derby: ERROR 42X35: It is not allowed for both operands of '>' to be ? parameters.
            expectSQLException(e, "derby");
        }
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").ge("James")).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood", "Cooper", "First")), new HashSet<String>(list));
        } catch (SQLException e) {
            final Class<? extends Dialect> dialectClass = getDialectDataSource().getDialect().getClass();
            // derby: ERROR 42X35: It is not allowed for both operands of '>=' to be ? parameters.
            expectSQLException(e, "derby");
        }
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").lt("James")).list(getDialectDataSource());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '<' to be ? parameters.
            expectSQLException(e, "derby");
        }
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("James").le("Margaret")).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("March", "Pedersen", "Redwood", "Cooper", "First")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '<=' to be ? parameters.
            expectSQLException(e, "derby");
        }
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").exceptAll(employee.firstName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support EXCEPT
            expectSQLException(e, "derby", "mysql");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").except(employee.firstName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            final Class<? extends Dialect> dialectClass = getDialectDataSource().getDialect().getClass();
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support EXCEPT
            expectSQLException(e, "derby", "mysql");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").exceptDistinct(employee.firstName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support EXCEPT
            expectSQLException(e, "derby", "mysql");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
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
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
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
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
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
        } catch (SQLException e) {
            final Class<? extends Dialect> dialectClass = getDialectDataSource().getDialect().getClass();
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").intersectAll(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support INTERSECT
            expectSQLException(e, "derby", "mysql");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").intersect(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support INTERSECT
            expectSQLException(e, "derby", "mysql");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").intersectDistinct(department.manager().lastName).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support INTERSECT
            expectSQLException(e, "derby", "mysql");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testSelectForUpdate() throws Exception {
        try {
            final List<String> list = Params.p("Redwood").forUpdate().list(getDialectDataSource());
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        }
    }

    public void testSelectForReadOnly() throws Exception {
        try {
            final List<String> list = Params.p("Redwood").forReadOnly().list(getDialectDataSource());
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        }
    }

    public void testExists() throws Exception {
        final Country country = new Country();
        try {
            final List<String> list = country.code.where(Params.p("Redwood").exists()).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("RUS", "USA", "FRA")), new HashSet<String>(list));
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        }

    }

    public void testExistsWithCondition() throws Exception {
        final Country country = new Country();
        final Department department = new Department();
        try {
            final List<String> list = country.code.where(Params.p("Redwood").where(department.countryId.eq(country.countryId)).exists()).list(getDialectDataSource());
            assertEquals(new HashSet<String>(Arrays.asList("RUS", "USA")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        }

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
           // derby: ERROR 42X35: It is not allowed for both operands of 'IN' to be ? parameters.
            expectSQLException(e, "derby");
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
           // derby: ERROR 42X35: It is not allowed for both operands of 'IN' to be ? parameters.
            expectSQLException(e, "derby");
        }
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("James").isNull()).list(getDialectDataSource());
        assertEquals(0, list.size());
    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("James").isNotNull()).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        System.out.println(employee.lastName.orderBy(Params.p("James")).show(getDialectDataSource().getDialect()));
        try {
            final List<String> list = employee.lastName.orderBy(Params.p("James")).list(getDialectDataSource());
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        }
    }

    public void testOrderByTwoColumns() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(Params.p("James"), employee.lastName).list(getDialectDataSource());
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Pedersen", "March", "Cooper", "First", "Redwood"));
            Collections.sort(expected);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        }
    }

    public void testOrderByNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(Params.p("James").nullsFirst()).list(getDialectDataSource());
            assertEquals("Cooper", list.get(0));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support NULLS FIRST
            expectSQLException(e, "derby", "mysql");
        }
    }

    public void testOrderByNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(Params.p("James").nullsLast()).list(getDialectDataSource());
            assertEquals("Cooper", list.get(list.size()-1));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support NULLS FIRST
            expectSQLException(e, "derby", "mysql");
        }
    }

    public void testOrderByAsc() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(Params.p("James").asc()).list(getDialectDataSource());
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");
        }
    }

    public void testOrderByDesc() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(Params.p("James").desc()).list(getDialectDataSource());
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "First", "Pedersen", "Redwood", "Cooper"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            final Class<? extends Dialect> dialectClass = getDialectDataSource().getDialect().getClass();
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby", "mysql");
        }
    }

    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = Params.p(1).opposite().where(employee.lastName.eq("Redwood")).list(getDialectDataSource());
            assertEquals(Arrays.asList(-1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, "derby");

        }
    }

    public void testPlus() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = Params.p(1).plus(employee.deptId).list(getDialectDataSource());
        final List<Integer> ids = employee.deptId.list(getDialectDataSource());
        assertEquals(list.size(), ids.size());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Integer id : ids) {
            expected.add(id == null ? null : id + 1);
        }
        assertEquals(expected, actual);
    }

    public void testMinus() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = Params.p(100).minus(employee.deptId).list(getDialectDataSource());
        final List<Integer> ids = employee.deptId.list(getDialectDataSource());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Integer id : ids) {
            expected.add(id == null ? null : 100 - id);
        }
        assertEquals(expected, actual);
    }

    public void testMult() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = Params.p(2).mult(employee.deptId).list(getDialectDataSource());
        final List<Integer> ids = employee.deptId.list(getDialectDataSource());
        assertEquals(list.size(), ids.size());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Integer id : ids) {
            expected.add(id == null ? null : 2 * id);
        }
        assertEquals(expected, actual);
    }

    public void testDiv() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = Params.p(100).div(employee.deptId).list(getDialectDataSource());
        final List<Integer> ids = employee.deptId.list(getDialectDataSource());
        assertEquals(list.size(), ids.size());
        final Set<Integer> actual = new HashSet<Integer>();
        for (Number n: list) {
            actual.add(n == null ? null : n.intValue());
        }
        final Set<Integer> expected = new HashSet<Integer>();
        for (Integer id : ids) {
            expected.add(id == null ? null : 100 / id);
        }
        assertEquals(expected, actual);
    }

    public void testPlusNumber() throws Exception {
        try {
            final List<Number> sums = Params.p(2).plus(1).list(getDialectDataSource());
            assertEquals(1, sums.size());
            assertEquals(3, sums.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '+' to be ? parameters.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testMinusNumber() throws Exception {
        try {
            final List<Number> sums = Params.p(3).minus(2).list(getDialectDataSource());
            assertEquals(1, sums.size());
            assertEquals(1, sums.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '-' to be ? parameters.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testMultNumber() throws Exception {
        try {
            final List<Number> sums = Params.p(3).mult(2).list(getDialectDataSource());
            assertEquals(1, sums.size());
            assertEquals(6, sums.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '*' to be ? parameters.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testDivNumber() throws Exception {
        try {
            final List<Number> sums = Params.p(6).div(2).list(getDialectDataSource());
            assertEquals(1, sums.size());
            assertEquals(3, sums.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '/' to be ? parameters.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testConcat() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = Params.p("Ms. ").concat(employee.lastName).where(employee.lastName.eq("Redwood")).list(getDialectDataSource());
        assertEquals(Arrays.asList("Ms. Redwood"), list);
    }

    public void testConcatString() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = Params.p("Success").concat(" expected").where(employee.lastName.eq("Redwood")).list(getDialectDataSource());
            assertEquals(Arrays.asList("Success expected"), list);
        } catch (SQLException e) {
            // ERROR 42X35: It is not allowed for both operands of '||' to be ? parameters.
            expectSQLException(e, "derby");
        }
    }

    public void testCount() throws Exception {
        try {
            final List<Integer> list = Params.p("Oops").count().list(getDialectDataSource());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'COUNT' operator is not allowed to take a ? parameter as an operand
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            expectIllegalStateException(e, GenericDialect.class);
        }

    }

    public void testCountDistinct() throws Exception {
        try {
            final List<Integer> list = Params.p("Oops").countDistinct().list(getDialectDataSource());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'COUNT' operator is not allowed to take a ? parameter as an operand
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            expectIllegalStateException(e, GenericDialect.class);
        }

    }

    public void testAvg() throws Exception {
        try {
            final List<Number> list = Params.p(1).avg().list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'AVG' operator is not allowed to take a ? parameter as an operand.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testSum() throws Exception {
        try {
            final List<Number> list = Params.p(1L).sum().list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'SUM' operator is not allowed to take a ? parameter as an operand.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testMin() throws Exception {
        try {
            final List<Integer> list = Params.p(1).min().list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'MIN' operator is not allowed to take a ? parameter as an operand.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testMax() throws Exception {
        try {
            final List<Integer> list = Params.p(1).max().list(getDialectDataSource());
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'MAX' operator is not allowed to take a ? parameter as an operand.
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testQueryValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Pair<String, String>> list = Params.p("X").queryValue().pair(employee.lastName)
                    .orderBy(employee.lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(Pair.make("X", "Cooper"), Pair.make("X", "First"), Pair.make("X", "March"), Pair.make("X", "Pedersen"), Pair.make("X", "Redwood")), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed
            expectSQLException(e, "derby");
        } catch (IllegalStateException e) {
            expectIllegalStateException(e, GenericDialect.class);
        }
    }


    public void testLike() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("James").like("%es")).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("First", "Cooper", "Redwood", "March", "Pedersen"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testLikeWithNumericArguments() throws Exception {
        // Note: this is a workaround for derbyERROR 42X35: It is not allowed for both operands of '=' to be ? parameters:
        // 'LIKE' accepts 2 '?'s

        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p(11).like(11+"")).list(getDialectDataSource());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("First", "Cooper", "Redwood", "March", "Pedersen"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    public void testNotLikeWithNumericArguments() throws Exception {
        // Note: this is a workaround for derbyERROR 42X35: It is not allowed for both operands of '=' to be ? parameters:
        // 'LIKE' accepts 2 '?'s

        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p(11).notLike(11 + "")).list(getDialectDataSource());
        assertEquals(0, list.size());
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
