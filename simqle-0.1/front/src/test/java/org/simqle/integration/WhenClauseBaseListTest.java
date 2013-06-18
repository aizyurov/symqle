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

        // all nulls are different, so distinct must return 2 nulls
        try {
            assertEquals(Arrays.asList("(null)", "(null)", "high", "low"), noNulls);
        } catch (AssertionFailedError e) {
            // some databases treat multiple NULLs as the same in DISTINCT
            if ("mysql".equals(getDatabaseName()) || "derby".equals(getDatabaseName()))
                assertEquals(Arrays.asList("(null)", "high", "low"), noNulls);
            else
                throw e;
        }
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
}
