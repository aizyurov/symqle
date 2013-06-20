package org.simqle.integration;

import org.simqle.Mappers;
import org.simqle.Pair;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.integration.model.MyDual;
import org.simqle.integration.model.One;
import org.simqle.mysql.MysqlDialect;
import org.simqle.sql.AbstractValueExpressionPrimary;
import org.simqle.sql.GenericDialect;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class ValueExpressionPrimaryTest extends AbstractIntegrationTestBase {


    /**
     * Returns the alphabetically first last name of employees of this department
     * @param department
     * @return
     */
    private AbstractValueExpressionPrimary<String> creatPrimary(final Department department) {
        final Employee employee = new Employee();
        return employee.lastName.min().where(employee.deptId.eq(department.deptId)).queryValue();
    }

    public void testList() throws Exception {
        final MyDual myDual = new MyDual();
        final AbstractValueExpressionPrimary<String> primary = myDual.dummy.queryValue();
        final List<String> list = primary.list(getDatabaseGate());
        assertEquals(Arrays.asList("X"), list);
    }

    public void testMap() throws Exception {
        final One one = new One();
        final AbstractValueExpressionPrimary<String> primary = one.id.map(Mappers.STRING).queryValue();
        final List<String> list = primary.list(getDatabaseGate());
        assertEquals(Arrays.asList("1"), list);
    }

    public void testAll() throws Exception {
        final MyDual myDual = new MyDual();
        final AbstractValueExpressionPrimary<String> primary = myDual.dummy.queryValue();
        final List<String> list = primary.all().list(getDatabaseGate());
        assertEquals(Arrays.asList("X"), list);
    }

    public void testDistinct() throws Exception {
        final MyDual myDual = new MyDual();
        final AbstractValueExpressionPrimary<String> primary = myDual.dummy.queryValue();
        final List<String> list = primary.distinct().list(getDatabaseGate());
        assertEquals(Arrays.asList("X"), list);
    }


    public void testWhere() throws Exception {
        final Department department = new Department();
        final List<String> list = creatPrimary(department).where(department.deptName.eq("HR")).list(getDatabaseGate());
        assertEquals(Arrays.asList("March"), list);
    }

    public void testOrderBy() throws Exception {
        final Department department = new Department();
        final List<String> list = creatPrimary(department).orderBy(department.deptName).list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "March"), list);
    }

    public void testPair() throws Exception {
        final Department department = new Department();
        final List<Pair<String, String>> list = creatPrimary(department).pair(department.deptName).orderBy(department.deptName.desc()).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make("March", "HR"), Pair.make("First", "DEV")), list);
    }


    public void testIsNull() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).isNull())
                .list(getDatabaseGate());
        assertEquals(0, list.size());
    }

    public void testIsNotNull() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).isNotNull())
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testEqValue() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).eq("March"))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("HR"), list);
    }

    public void testNeValue() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).ne("March"))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testLeValue() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).le("March"))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testLtValue() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).lt("March"))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testGeValue() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).ge("March"))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("HR"), list);
    }

    public void testGtValue() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).gt("March"))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(), list);
    }

    public void testEq() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).eq(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testNe() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).ne(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("HR"), list);
    }

    public void testGe() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).ge(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testGt() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).gt(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(), list);
    }

    public void testLe() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).le(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testLt() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).lt(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("HR"), list);
    }

    public void testIn() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).in(new Department().manager().lastName))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testNotIn() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).notIn(new Department().manager().lastName))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("HR"), list);
    }

    public void testInList() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).in("Cooper", "March"))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("HR"), list);
    }

    public void testNotInList() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).notIn("Cooper", "March"))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testOpposite() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.max().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                primary.opposite().pair(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(-3000.0, "DEV"), Pair.make(-3000.0, "HR")), list);
    }

    public void testAdd() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Number, String>> list =
                primary.add(department.manager().salary).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(5000.0, list.get(0).first().doubleValue());
        assertEquals("DEV", list.get(0).second());
        assertEquals(2, list.size());
    }

    public void testSub() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Number, String>> list =
                primary.sub(department.manager().salary).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(-1000.0, list.get(0).first().doubleValue());
        assertEquals("DEV", list.get(0).second());
        assertEquals(2, list.size());
    }

    public void testMult() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Number, String>> list =
                primary.mult(department.manager().salary).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(6000000.0, list.get(0).first().doubleValue());
        assertEquals("DEV", list.get(0).second());
        assertEquals(2, list.size());
    }

    public void testDiv() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.max().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Number, String>> list =
                primary.div(department.manager().salary).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(1.0, list.get(0).first().doubleValue());
        assertEquals("DEV", list.get(0).second());
        assertEquals(2, list.size());
    }

    public void testAddNumber() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Number, String>> list =
                primary.add(500.0).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(2500.0, list.get(0).first().doubleValue());
        assertEquals("DEV", list.get(0).second());
        assertEquals(2, list.size());
    }

    public void testSubNumber() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Number, String>> list =
                primary.sub(500.0).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(1500.0, list.get(0).first().doubleValue());
        assertEquals("DEV", list.get(0).second());
        assertEquals(2, list.size());
    }

    public void testMultNumber() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Number, String>> list =
                primary.mult(2).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(4000.0, list.get(0).first().doubleValue());
        assertEquals("DEV", list.get(0).second());
        assertEquals(2, list.size());
    }

    public void testDivNumber() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Number, String>> list =
                primary.div(2).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(1000.0, list.get(0).first().doubleValue());
        assertEquals("DEV", list.get(0).second());
        assertEquals(2, list.size());
    }

    public void testBooleanValue() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<String> list = department.deptName
                .where(employee.firstName.eq("Margaret").asValue().where(employee.lastName.eq(department.manager().lastName)).queryValue().booleanValue())
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("HR"), list);
    }

    public void testConcat() throws Exception {
        final Department department = new Department();
        final List<String> list = creatPrimary(department).concat(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("FirstDEV", "MarchHR"), list);
    }

    public void testConcatString() throws Exception {
        final Department department = new Department();
        final List<String> list = creatPrimary(department).concat("-").concat(department.deptName)
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First-DEV", "March-HR"), list);
    }

    public void testOrderByArgument() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .orderBy(creatPrimary(department))
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testOrderByNullsFirst() throws Exception {
        try {
            final Department department = new Department();
            final List<String> list = department.deptName
                    .orderBy(creatPrimary(department).nullsFirst())
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("DEV", "HR"), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByNullsLast() throws Exception {
        try {
            final Department department = new Department();
            final List<String> list = department.deptName
                    .orderBy(creatPrimary(department).nullsLast())
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("DEV", "HR"), list);
        } catch (SQLException e) {
            // mysql does not support NULLS LAST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByAsc() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .orderBy(creatPrimary(department).asc())
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testOrderByDesc() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .orderBy(creatPrimary(department).desc())
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("HR", "DEV"), list);
    }

    public void testUnionAll() throws Exception {
        final Department department = new Department();
        final List<String> list = creatPrimary(department).where(department.deptId.isNotNull()).unionAll(new Department().manager().lastName).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "First", "March", "Redwood"), list);
    }


    public void testUnionDistinct() throws Exception {
        final Department department = new Department();
        final List<String> list = creatPrimary(department).where(department.deptId.isNotNull()).unionDistinct(new Department().manager().lastName).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "March", "Redwood"), list);

    }

    public void testUnion() throws Exception {
        final Department department = new Department();
        final List<String> list = creatPrimary(department).where(department.deptId.isNotNull()).union(new Department().manager().lastName).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "March", "Redwood"), list);

    }

    public void testExceptAll() throws Exception {
        try {
            final Department department = new Department();
            final List<String> list = creatPrimary(department).where(department.deptId.isNotNull()).exceptAll(new Department().manager().lastName).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("March"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }

    }

    public void testExceptDistinct() throws Exception {
        try {
            final Department department = new Department();
            final List<String> list = creatPrimary(department).where(department.deptId.isNotNull()).exceptDistinct(new Department().manager().lastName).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("March"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }

    }

    public void testExcept() throws Exception {
        try {
            final Department department = new Department();
            final List<String> list = creatPrimary(department).where(department.deptId.isNotNull()).exceptDistinct(new Department().manager().lastName).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("March"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        try {
            final Department department = new Department();
            final List<String> list = creatPrimary(department).where(department.deptId.isNotNull()).intersectAll(new Department().manager().lastName).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final Department department = new Department();
            final List<String> list = creatPrimary(department).where(department.deptId.isNotNull()).intersectDistinct(new Department().manager().lastName).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersect() throws Exception {
        try {
            final Department department = new Department();
            final List<String> list = creatPrimary(department).where(department.deptId.isNotNull()).intersect(new Department().manager().lastName).list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testExists() throws Exception {
        try {
            final Department department = new Department();
            final List<String> list = department.deptName.where(creatPrimary(department).exists())
                    .orderBy(department.deptName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("DEV", "HR"), list);
        } catch (IllegalStateException e) {
            // Generic dialect does not support selects with no tables
            expectIllegalStateException(e, GenericDialect.class);
        }
    }

    public void testForUpdate() throws Exception {
        try {
            final One one = new One();
            final List<Integer> list = one.id.queryValue().forUpdate().list(getDatabaseGate());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            expectSQLException(e, "derby");
        }
    }

    public void testForReadOnly() throws Exception {
        try {
            final One one = new One();
            final List<Integer> list = one.id.queryValue().forReadOnly().list(getDatabaseGate());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            if (MysqlDialect.class.equals(getDatabaseGate().getDialect().getClass())) {
                // should work with MysqlDialect
                throw e;
            } else {
                // mysql does not support FOR READ ONLY natively
                expectSQLException(e, "mysql");
            }
        }
    }

    public void testQueryValue() throws Exception {
        try {
            final One one = new One();
            final List<Integer> list = one.id.queryValue().queryValue().list(getDatabaseGate());
            assertEquals(Arrays.asList(1), list);
        } catch (IllegalStateException e) {
            expectIllegalStateException(e, GenericDialect.class);
        }
    }


    public void testWhenClause() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName.eq("DEV").then(creatPrimary(department)).orElse(department.manager().lastName)
                .list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testLike() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).like(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testLikeString() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).like("_ar%"))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("HR"), list);
    }

    public void testNotLike() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).notLike(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("HR"), list);
    }

    public void testNotLikeString() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(creatPrimary(department).notLike("_ar%"))
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV"), list);
    }

    public void testCount() throws Exception {
        final Department department = new Department();
        final List<Integer> list = creatPrimary(department).count()
                .where(department.deptId.isNotNull())
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(2), list);
    }

    public void testCountDistinct() throws Exception {
        final Department department = new Department();
        final List<Integer> list = creatPrimary(department).countDistinct()
                .where(department.deptId.isNotNull())
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(2), list);
    }

    public void testAverage() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.max().where(employee.deptId.eq(department.deptId)).queryValue().avg().where(department.deptId.isNotNull()).list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(3000.0, list.get(0).doubleValue());
    }

    public void testMin() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.max().where(employee.deptId.eq(department.deptId)).queryValue().min().where(department.deptId.isNotNull()).list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(3000.0, list.get(0));
    }

    public void testMax() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final List<Double> list = employee.salary.max().where(employee.deptId.eq(department.deptId)).queryValue().max().where(department.deptId.isNotNull()).list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(3000.0, list.get(0));
    }
}
