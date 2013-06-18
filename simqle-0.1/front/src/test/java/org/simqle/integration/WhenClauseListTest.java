package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.integration.model.Employee;
import org.simqle.sql.AbstractSearchedWhenClauseList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class WhenClauseListTest extends AbstractIntegrationTestBase {

    private AbstractSearchedWhenClauseList<String> createWhenClauseList(final Employee employee) {
        return employee.salary.gt(2500.0).then(Params.p("high")).orWhen(employee.salary.lt(1800.0).then(Params.p("low"))).orElse(employee.firstName);
    }

    private AbstractSearchedWhenClauseList<Double> createNumericWCL(final Employee employee) {
        return employee.empId.eq(employee.department().manager().empId).then(employee.salary).orWhen(employee.retired.booleanValue().then(employee.salary.opposite())).orElse(employee.department().manager().salary);
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> list = createWhenClauseList(employee)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of("low", "Cooper"),
                Pair.of("high", "First"),
                Pair.of("Bill", "March"),
                Pair.of("Alex", "Pedersen"),
                Pair.of("high", "Redwood")
        ), list);
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                "low",
                "high",
                "Bill",
                "Alex",
                "high"
        ), list);
    }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .where(employee.firstName.eq("James"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                "low",
                "high"
        ), list);
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .list(getDialectDataSource());

        assertEquals(Arrays.asList("Alex", "Bill", "high", "high", "low"), replaceNullsAndSort(list));
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .all()
                .list(getDialectDataSource());

        assertEquals(Arrays.asList("Alex", "Bill", "high", "high", "low"), replaceNullsAndSort(list));
    }

    public void testDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .distinct()
                .list(getDialectDataSource());
        final List<String> noNulls = replaceNullsAndSort(list);

        assertEquals(Arrays.asList("Alex", "Bill", "high", "low"), noNulls);
    }

    private List<String> replaceNullsAndSort(final List<String> list) {
        final List<String> noNulls = new ArrayList<String>(list.size());
        for (String s: list) {
            noNulls.add(s == null ? "(null)" : s);
        }
        Collections.sort(noNulls);
        return noNulls;
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = whenClauseList.forUpdate().list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "James", "Redwood", "nobody", "nobody"), replaceNullsAndSort(list));

    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList =
                createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.forReadOnly().list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "James", "Redwood", "nobody", "nobody"), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support FOR READ ONLY
            expectSQLException(e, "mysql");
        }

    }


    public void testThenNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = employee.salary.gt(2500.0).then(Params.p("high")).orWhen(employee.salary.lt(1800.0).thenNull()).orElse(Params.p("medium"));
        final List<Pair<String, String>> list = whenClauseList
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of((String) null, "Cooper"),
                Pair.of("high", "First"),
                Pair.of("medium", "March"),
                Pair.of("medium", "Pedersen"),
                Pair.of("high", "Redwood")
        ), list);
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).eq("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).ne("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).gt("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).ge("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).lt("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).le("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testInArgument() throws Exception {
        final Employee pattern = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(pattern);
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.in(whenClauseList))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = employee.lastName.where(whenClauseList.in(james.lastName.where(james.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First"), list);
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = employee.lastName.where(whenClauseList.notIn(james.lastName.where(james.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "Redwood"), list);
    }
    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).in("high", (String) null))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        // NULLs should not match to (String)null
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).notIn("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        // for NULLs NOT IN is false
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testAsElseArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.eq("Bill").then(employee.firstName)
                .orElse(createWhenClauseList(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of("low", "Cooper"),
                Pair.of("high", "First"),
                Pair.of("Bill", "March"),
                Pair.of("Alex", "Pedersen"),
                Pair.of("high", "Redwood")
        ), list);
    }

    public void testAsSortSpec() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseList(employee), employee.lastName)
                .list(getDialectDataSource());
        // sort order by first field: Alex, Bill, high, high, low
        assertEquals(Arrays.asList("Pedersen", "March", "First", "Redwood", "Cooper"), list);
    }

    public void testNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClauseList(employee).nullsFirst(), employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Pedersen", "March", "First", "Redwood", "Cooper"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClauseList(employee).nullsLast(), employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Pedersen", "March", "First", "Redwood", "Cooper"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS LAST:
                // You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'NULLS LAST
            expectSQLException(e, "mysql");
        }
    }

    public void testAsc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseList(employee).asc(), employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Pedersen", "March", "First", "Redwood", "Cooper"), list);
    }

    public void testDesc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseList(employee).desc(), employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood", "March", "Pedersen"), list);
    }

    // TODO: derby objects if the argument of second THEN is ?
    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList.opposite().pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of(1500.0, "Cooper"),
                Pair.of(-3000.0, "First"),
                Pair.of(-3000.0, "March"),
                Pair.of(-3000.0, "Pedersen"),
                Pair.of(-3000.0, "Redwood")
        ), list);
    }

    public void testPlus() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Number, String>> list = whenClauseList.plus(100.0).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of(-1400.0, "Cooper"),
                Pair.of(3100.0, "First"),
                Pair.of(3100.0, "March"),
                Pair.of(3100.0, "Pedersen"),
                Pair.of(3100.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    public void testMinus() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Number, String>> list = whenClauseList.minus(100.0).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of(-1600.0, "Cooper"),
                Pair.of(2900.0, "First"),
                Pair.of(2900.0, "March"),
                Pair.of(2900.0, "Pedersen"),
                Pair.of(2900.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    public void testMult() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Number, String>> list = whenClauseList.mult(2).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of(-3000.0, "Cooper"),
                Pair.of(6000.0, "First"),
                Pair.of(6000.0, "March"),
                Pair.of(6000.0, "Pedersen"),
                Pair.of(6000.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    public void testDiv() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Number, String>> list = whenClauseList.div(3).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of(-500.0, "Cooper"),
                Pair.of(1000.0, "First"),
                Pair.of(1000.0, "March"),
                Pair.of(1000.0, "Pedersen"),
                Pair.of(1000.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    private List<Pair<Double, String>> convertToDoubleStringPairList(List<Pair<Number, String>> source) {
        final List<Pair<Double, String>> result = new ArrayList<Pair<Double, String>>();
        for (Pair<Number, String> p : source) {
            result.add(Pair.of(p.getFirst() == null ? null : p.getFirst().doubleValue(), p.getSecond()));
        }
        return result;
    }

    public void testConcat() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClauseList(employee).concat("+")
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of("low+", "Cooper"),
                Pair.of("high+", "First"),
                Pair.of("Bill+", "March"),
                Pair.of("Alex+", "Pedersen"),
                Pair.of("high+", "Redwood")
        ), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = whenClauseList.unionAll(new Employee().lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(
                "Cooper", "First", "First", "James", "March", "Pedersen", "Redwood", "Redwood", "nobody", "nobody"
        ), replaceNullsAndSort(list));
    }

    private AbstractSearchedWhenClauseList<String> createNamesWCL(final Employee employee) {
        return employee.salary.gt(2500.0).then(employee.lastName)
                .orWhen(employee.salary.lt(1800.0).then(employee.firstName)).orElse(Params.p("nobody"));
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = whenClauseList.unionDistinct(new Employee().lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(
                "Cooper", "First", "James", "March", "Pedersen", "Redwood", "nobody"
        ), replaceNullsAndSort(list));
    }


    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = whenClauseList.union(new Employee().lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(
                "Cooper", "First", "James", "March", "Pedersen", "Redwood", "nobody"
        ), replaceNullsAndSort(list));
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.exceptAll(new Employee().lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    "James", "nobody", "nobody"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.exceptDistinct(new Employee().lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    "James", "nobody"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.except(new Employee().lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    "James", "nobody"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.intersectAll(new Employee().lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    "First", "Redwood"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.intersectDistinct(new Employee().lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    "First", "Redwood"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.intersect(new Employee().lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    "First", "Redwood"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testCount() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClauseList(employee).count().list(getDialectDataSource());
        // only NOT NULL values are counted
        assertEquals(Arrays.asList(5), list);
    }

    public void testCountDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClauseList(employee).count().list(getDialectDataSource());
        assertEquals(Arrays.asList(5), list);
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee).min().list(getDialectDataSource());
        assertEquals(Arrays.asList("Alex"), list);
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee).max().list(getDialectDataSource());
        assertEquals(Arrays.asList("low"), list);
    }

    public void testSum() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Number> list = whenClauseList.sum().list(getDialectDataSource());
        assertEquals(1, list.size());
        assertEquals(10500.0, list.get(0).doubleValue());

    }

    public void testAvg() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Number> list = whenClauseList.avg().list(getDialectDataSource());
        assertEquals(1, list.size());
        // average is calculated over NOT NULL values only
        assertEquals(2100.0, list.get(0).doubleValue());
    }
}
