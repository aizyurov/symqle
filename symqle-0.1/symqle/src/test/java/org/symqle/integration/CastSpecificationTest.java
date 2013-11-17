package org.symqle.integration;

import org.symqle.common.Mappers;
import org.symqle.common.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractCastSpecification;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class CastSpecificationTest extends AbstractIntegrationTestBase {



    private AbstractCastSpecification<Double> createCast(final Employee employee) {
        return employee.salary.cast("DECIMAL(6,2)");
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).selectAll().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).distinct().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).orderBy(employee.lastName).list(getEngine());
        // Cooper, First, March, Pedersen, Redwood
        assertEquals(Arrays.asList(1500.0, 3000.0, 2000.0, 2000.0, 3000.0), list);
    }

    public void testOrderAsc() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).orderAsc().list(getEngine());
        // Cooper, First, March, Pedersen, Redwood
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testOrderDesc() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).orderDesc().list(getEngine());
        // Cooper, First, March, Pedersen, Redwood
        assertEquals(Arrays.asList(3000.0, 3000.0, 2000.0, 2000.0, 1500.0), list);
    }

    public void testSortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .orderBy(createCast(employee), employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
    }

    public void testAsc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .orderBy(createCast(employee).asc(), employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
    }

    public void testDesc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .orderBy(createCast(employee).desc(), employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood", "March", "Pedersen", "Cooper"), list);
    }

    public void testNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(employee.deptId.cast("BIGINT").nullsFirst()).list(getEngine());
            assertEquals(5, list.size());
            assertEquals("Cooper", list.get(0));
        } catch (SQLException e) {
            // mysql;: does not support NULLS FIRST
            expectSQLException(e, "MySQL");
        }
    }

    public void testNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(employee.deptId.cast("BIGINT").nullsLast()).list(getEngine());
            assertEquals(5, list.size());
            assertEquals("Cooper", list.get(4));
        } catch (SQLException e) {
            // mysql;: does not support NULLS LAST
            expectSQLException(e, "MySQL");
        }
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double,String>> list = createCast(employee).pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(1500.0, "Cooper"),
                Pair.make(3000.0, "First"),
                Pair.make(2000.0, "March"),
                Pair.make(2000.0, "Pedersen"),
                Pair.make(3000.0, "Redwood")
                ), list);
    }

    public void testAsCondition() throws Exception {
        final Employee employee = new Employee();
        final String castTarget = "MySQL".equals(getDatabaseName()) ? "DECIMAL(1)" : "BOOLEAN";
        final List<String> list = employee.lastName
                .where(employee.retired.cast(castTarget).asPredicate())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).where(employee.firstName.eq("James")).orderBy(employee.lastName).list(getEngine());
        // Cooper, First
        assertEquals(Arrays.asList(1500.0, 3000.0), list);
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).eq(3000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).ne(3000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).gt(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).ge(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).lt(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).le(2000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.deptId.cast("DECIMAL(4)").isNull()).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.deptId.cast("DECIMAL(4)").isNotNull())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee sample = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).in(sample.salary.where(sample.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).in(1500.0, 3000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee sample = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).notIn(sample.salary.where(sample.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createCast(employee).notIn(1500.0, 3000.0))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    public void testInArgument() throws Exception {
        final Employee employee = new Employee();
        final Employee another = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.in(createCast(another)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testAdd() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).add(100).map(Mappers.DOUBLE)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(1600.0, 3100.0, 2100.0, 2100.0, 3100.0), list);
    }

    public void testSub() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).sub(100).map(Mappers.DOUBLE)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(1400.0, 2900.0, 1900.0, 1900.0, 2900.0), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).unionAll(employee.salary.where(employee.lastName.eq("Redwood")))
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0, 3000.0), list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = createCast(employee).unionDistinct(employee.salary.where(employee.lastName.eq("Redwood")))
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Double> list = createCast(employee).intersect(employee.salary.where(employee.lastName.eq("Redwood")))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList(3000.0), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testConcat() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.cast("CHAR(5)").concat(employee.lastName)
                .where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(Arrays.asList("JamesCooper"), list);
    }

    public void testCollate() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.firstName.cast("CHAR(5)").map(Mappers.STRING).collate("utf8mb4_unicode_ci")
                    .where(employee.lastName.eq("Cooper"))
                    .list(getEngine());
            assertEquals(Arrays.asList("James"), list);
        } catch (SQLException e) {
            // derby: does not support COLLATE
            System.out.println(getDatabaseName());
            expectSQLException(e, "Apache Derby");
        }
    }

}
