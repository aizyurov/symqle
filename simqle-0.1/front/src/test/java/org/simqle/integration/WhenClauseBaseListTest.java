package org.simqle.integration;

import junit.framework.AssertionFailedError;
import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.integration.model.Employee;
import org.simqle.sql.AbstractSearchedWhenClauseBaseList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class WhenClauseBaseListTest extends AbstractIntegrationTestBase {

    private AbstractSearchedWhenClauseBaseList<String> createWhenClauseBaseList(final Employee employee) {
        return employee.salary.gt(2500.0).then(Params.p("high")).orWhen(employee.salary.lt(1800.0).then(Params.p("low")));
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> list = createWhenClauseBaseList(employee)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of("low", "Cooper"),
                Pair.of("high", "First"),
                Pair.of((String)null, "March"),
                Pair.of((String)null, "Pedersen"),
                Pair.of("high", "Redwood")
        ), list);
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                "low",
                "high",
                (String)null,
                (String)null,
                "high"
        ), list);
    }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
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
        final List<String> list = createWhenClauseBaseList(employee)
                .list(getDialectDataSource());

        assertEquals(Arrays.asList("(null)", "(null)", "high", "high", "low"), replaceNullsAndSort(list));
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .all()
                .list(getDialectDataSource());

        assertEquals(Arrays.asList("(null)", "(null)", "high", "high", "low"), replaceNullsAndSort(list));
    }

    public void testDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .distinct()
                .list(getDialectDataSource());
        final List<String> noNulls = replaceNullsAndSort(list);

        assertEquals(Arrays.asList("(null)", "high", "low"), noNulls);
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
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = whenClauseBaseList.forUpdate().list(getDialectDataSource());
        assertEquals(Arrays.asList("(null)", "(null)", "First", "James", "Redwood"), replaceNullsAndSort(list));

    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.forReadOnly().list(getDialectDataSource());
            assertEquals(Arrays.asList("(null)", "(null)", "First", "James", "Redwood"), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support FOR READ ONLY
            expectSQLException(e, "mysql");
        }

    }


    public void testElseParam() throws Exception {
    final Employee employee = new Employee();
        try {
            final List<Pair<String,String>> list = createWhenClauseBaseList(employee).orElse(Params.p("medium"))
                    .pair(employee.lastName)
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    Pair.of("low", "Cooper"),
                    Pair.of("high", "First"),
                    Pair.of("medium", "March"),
                    Pair.of("medium", "Pedersen"),
                    Pair.of("high", "Redwood")
            ), list);
        } catch (SQLException e) {
            // derby: ERROR 42X87: At least one result expression (THEN or ELSE) of the 'conditional' expression must not be a '?'.
            expectSQLException(e, "derby");
        }
    }

    public void testElse() throws Exception {
    final Employee employee = new Employee();
        final List<Pair<String,String>> list = createWhenClauseBaseList(employee).orElse(employee.firstName)
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

    public void testChain() throws Exception {
        final Employee employee = new Employee();
            final List<Pair<String,String>> list = createWhenClauseBaseList(employee)
                    .orWhen(employee.firstName.eq("Bill").then(Params.p("medium")))
                    .orElse(employee.firstName)
                    .pair(employee.lastName)
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    Pair.of("low", "Cooper"),
                    Pair.of("high", "First"),
                    Pair.of("medium", "March"),
                    Pair.of("Alex", "Pedersen"),
                    Pair.of("high", "Redwood")
            ), list);
    }

    public void testThenNull() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList = employee.salary.gt(2500.0).then(Params.p("high")).orWhen(employee.salary.lt(1800.0).thenNull());
        final List<Pair<String, String>> list = whenClauseBaseList.orElse(Params.p("medium"))
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
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).eq("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).ne("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).gt("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).ge("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).lt("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).le("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testInArgument() throws Exception {
        final Employee pattern = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                pattern.salary.gt(2500.0).then(pattern.lastName)
                        .orWhen(pattern.salary.lt(1800.0).then(pattern.firstName));
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.in(whenClauseBaseList))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = employee.lastName.where(whenClauseBaseList.in(james.lastName.where(james.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First"), list);
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = employee.lastName.where(whenClauseBaseList.notIn(james.lastName.where(james.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "Redwood"), list);
    }
    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).in("high", (String) null))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        // NULLs should not match to (String)null
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).notIn("high"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        // for NULLs NOT IN is false
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testAsElseArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.eq("Bill").then(employee.firstName)
                .orElse(createWhenClauseBaseList(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of("low", "Cooper"),
                Pair.of("high", "First"),
                Pair.of("Bill", "March"),
                Pair.of((String)null, "Pedersen"),
                Pair.of("high", "Redwood")
        ), list);
    }

    public void testAsSortSpec() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseBaseList(employee), employee.lastName)
                .list(getDialectDataSource());
        // order is unspecified;
        // assume NULLS LAST by default
        try {
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (AssertionFailedError e) {
            // mysql: default is NULLS FIRST
            if ("mysql".equals(getDatabaseName())) {
                assertEquals(Arrays.asList("March", "Pedersen", "First", "Redwood", "Cooper"), list);
            } else {
                throw e;
            }
        }
    }

    public void testNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClauseBaseList(employee).nullsFirst(), employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Pedersen", "First", "Redwood", "Cooper"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClauseBaseList(employee).nullsLast(), employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS LAST:
                // You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'NULLS LAST
            expectSQLException(e, "mysql");
        }
    }

    public void testAsc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseBaseList(employee).asc(), employee.lastName)
                .list(getDialectDataSource());
        // order is unspecified;
        // assume NULLS LAST by default
        try {
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (AssertionFailedError e) {
            // mysql: default is NULLS FIRST
            if ("mysql".equals(getDatabaseName())) {
                assertEquals(Arrays.asList("March", "Pedersen", "First", "Redwood", "Cooper"), list);
            } else {
                throw e;
            }
        }
    }

    public void testDesc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseBaseList(employee).desc(), employee.lastName)
                .list(getDialectDataSource());
        // order is unspecified;
        // assume NULLS to be at the end for DESC
        try {
            assertEquals(Arrays.asList("March", "Pedersen", "Cooper", "First", "Redwood"), list);
        } catch (AssertionFailedError e) {
            // mysql: default is NULLS FIRST, reversed for DESC
            if ("mysql".equals(getDatabaseName())) {
                assertEquals(Arrays.asList("Cooper", "First", "Redwood", "March", "Pedersen"), list);
            } else {
                throw e;
            }
        }
    }

    // TODO: derby objects if the argument of second THEN is ?
    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = whenClauseBaseList.opposite().pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of(1500.0, "Cooper"),
                Pair.of(-3000.0, "First"),
                Pair.of((Double)null, "March"),
                Pair.of((Double)null, "Pedersen"),
                Pair.of(-3000.0, "Redwood")
        ), list);
    }

    public void testPlus() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Number, String>> list = whenClauseBaseList.plus(100.0).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of(-1400.0, "Cooper"),
                Pair.of(3100.0, "First"),
                Pair.of((Double)null, "March"),
                Pair.of((Double)null, "Pedersen"),
                Pair.of(3100.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    public void testMinus() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Number, String>> list = whenClauseBaseList.minus(100.0).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of(-1600.0, "Cooper"),
                Pair.of(2900.0, "First"),
                Pair.of((Double)null, "March"),
                Pair.of((Double)null, "Pedersen"),
                Pair.of(2900.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    public void testMult() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Number, String>> list = whenClauseBaseList.mult(2).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of(-3000.0, "Cooper"),
                Pair.of(6000.0, "First"),
                Pair.of((Double)null, "March"),
                Pair.of((Double)null, "Pedersen"),
                Pair.of(6000.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    public void testDiv() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Number, String>> list = whenClauseBaseList.div(3).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of(-500.0, "Cooper"),
                Pair.of(1000.0, "First"),
                Pair.of((Double)null, "March"),
                Pair.of((Double)null, "Pedersen"),
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
        final List<Pair<String, String>> list = createWhenClauseBaseList(employee).concat("+")
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(
                Pair.of("low+", "Cooper"),
                Pair.of("high+", "First"),
                Pair.of((String) null, "March"),
                Pair.of((String) null, "Pedersen"),
                Pair.of("high+", "Redwood")
        ), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = whenClauseBaseList.unionAll(new Employee().lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(
                "(null)", "(null)", "Cooper", "First", "First", "James", "March", "Pedersen", "Redwood", "Redwood"
        ), replaceNullsAndSort(list));
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = whenClauseBaseList.unionDistinct(new Employee().lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(
                "(null)", "Cooper", "First", "James", "March", "Pedersen", "Redwood"
        ), replaceNullsAndSort(list));
    }


    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = whenClauseBaseList.union(new Employee().lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList(
                "(null)", "Cooper", "First", "James", "March", "Pedersen", "Redwood"
        ), replaceNullsAndSort(list));
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.exceptAll(new Employee().lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    "(null)", "(null)", "James"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.exceptDistinct(new Employee().lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    "(null)", "James"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.except(new Employee().lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList(
                    "(null)", "James"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.intersectAll(new Employee().lastName).list(getDialectDataSource());
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
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.intersectDistinct(new Employee().lastName).list(getDialectDataSource());
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
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.intersect(new Employee().lastName).list(getDialectDataSource());
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
        final List<Integer> list = createWhenClauseBaseList(employee).count().list(getDialectDataSource());
        try {
            assertEquals(Arrays.asList(5), list);
        } catch (AssertionFailedError e) {
            // mysql counts only NOT NULL values
            if ("mysql".equals(getDatabaseName())) {
                assertEquals(Arrays.asList(3), list);
            } else {
                throw e;
            }
        }
    }

    public void testCountDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClauseBaseList(employee).count().list(getDialectDataSource());
        assertEquals(Arrays.asList(3), list);
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee).min().list(getDialectDataSource());
        assertEquals(Arrays.asList("high"), list);
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee).max().list(getDialectDataSource());
        assertEquals(Arrays.asList("low"), list);
    }

    public void testSum() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Number> list = whenClauseBaseList.sum().list(getDialectDataSource());
        assertEquals(1, list.size());
        assertEquals(4500.0, list.get(0).doubleValue());

    }

    private AbstractSearchedWhenClauseBaseList<Double> createNumericWCBL(final Employee employee) {
        return employee.empId.eq(employee.department().manager().empId).then(employee.salary).orWhen(employee.retired.booleanValue().then(employee.salary.opposite()));
    }

    public void testAvg() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Number> list = whenClauseBaseList.avg().list(getDialectDataSource());
        assertEquals(1, list.size());
        try {
            assertEquals(900.0, list.get(0).doubleValue());
        } catch (AssertionFailedError e) {
            // mysql calculates average over NOT NULL values only
            if ("mysql".equals(getDatabaseName())) {
                assertEquals(1500.0, list.get(0).doubleValue());
            } else {
                throw e;
            }
        }
    }
}
