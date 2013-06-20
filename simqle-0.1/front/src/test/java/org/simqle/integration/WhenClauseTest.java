package org.simqle.integration;

import junit.framework.AssertionFailedError;
import org.simqle.Mappers;
import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.integration.model.Employee;
import org.simqle.sql.AbstractSearchedWhenClause;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class WhenClauseTest extends AbstractIntegrationTestBase {

    private AbstractSearchedWhenClause<String> createWhenClause(final Employee employee) {
        return employee.empId.eq(employee.department().manager().empId).then(employee.firstName);
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> list = createWhenClause(employee)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make((String)null, "Cooper"),
                Pair.make("James", "First"),
                Pair.make((String)null, "March"),
                Pair.make((String)null, "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                (String)null,
                "James",
                (String)null,
                (String)null,
                "Margaret"
        ), list);
    }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .where(employee.firstName.eq("James"))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                (String) null,
                "James"
        ), list);
    }

    public void testMap() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee).map(Mappers.STRING)
                .list(getDatabaseGate());

        assertEquals(Arrays.asList("(null)", "(null)", "(null)", "James", "Margaret"), replaceNullsAndSort(list));
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .list(getDatabaseGate());

        assertEquals(Arrays.asList("(null)", "(null)", "(null)", "James", "Margaret"), replaceNullsAndSort(list));
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .all()
                .list(getDatabaseGate());

        assertEquals(Arrays.asList("(null)", "(null)", "(null)", "James", "Margaret"), replaceNullsAndSort(list));
    }

    public void testDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .distinct()
                .list(getDatabaseGate());
        final List<String> noNulls = replaceNullsAndSort(list);

        assertEquals(Arrays.asList("(null)", "James", "Margaret"), replaceNullsAndSort(list));
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
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.forUpdate().list(getDatabaseGate());
            assertEquals(Arrays.asList("(null)", "(null)", "(null)", "James", "Margaret"), replaceNullsAndSort(list));
        } catch (SQLException e) {
            //derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            expectSQLException(e, "derby");
        }

    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.forReadOnly().list(getDatabaseGate());
            assertEquals(Arrays.asList("(null)", "(null)", "(null)", "James", "Margaret"), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support FOR READ ONLY
            expectSQLException(e, "mysql");
        }

    }


    public void testElseParam() throws Exception {
    final Employee employee = new Employee();
        final List<Pair<String,String>> list = createWhenClause(employee).orElse(Params.p("Anonymous"))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make("Anonymous", "Cooper"),
                Pair.make("James", "First"),
                Pair.make("Anonymous", "March"),
                Pair.make("Anonymous", "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    public void testElse() throws Exception {
    final Employee employee = new Employee();
        final List<Pair<String,String>> list = createWhenClause(employee).orElse(employee.firstName.concat("+"))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make("James+", "Cooper"),
                Pair.make("James", "First"),
                Pair.make("Bill+", "March"),
                Pair.make("Alex+", "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).eq("James"))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First"), list);
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).ne("James"))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).gt("James"))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).ge("James"))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).lt("James"))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).le("James"))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First"), list);
    }

    public void testInArgument() throws Exception {
        final Employee pattern = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(pattern);
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.in(whenClause))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testExists() throws Exception {
        final Employee pattern = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(pattern);
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(whenClause.exists())
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testContains() throws Exception {
        final Employee pattern = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(pattern);
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(whenClause.contains("Margaret"))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);

        final List<String> emptyList = employee.lastName.where(whenClause.contains("Bill"))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(0, emptyList.size());

    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = employee.lastName.where(whenClause.in(james.firstName.where(james.firstName.like("J%"))))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("First"), list);
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = employee.lastName.where(whenClause.notIn(james.firstName.where(james.firstName.like("J%"))))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("Redwood"), list);
    }
    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).in("Margaret", (String) null))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        // NULLs should not match to (String)null
        assertEquals(Arrays.asList("Redwood"), list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        System.out.println(employee.lastName.where(createWhenClause(employee).notIn("nobody", "somebody"))
                .orderBy(employee.lastName).show(getDatabaseGate().getDialect()));
        try {
            final List<String> list = employee.lastName.where(createWhenClause(employee).notIn("nobody", "somebody"))
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            // for NULLs NOT IN is false
            assertEquals(Arrays.asList("First", "Redwood"), list);
        } catch (SQLException e) {
            // derby bug: java.sql.SQLException: The exception 'java.lang.NullPointerException' was thrown while evaluating an expression.
                // Caused by: java.lang.NullPointerException
//                	at org.apache.derby.exe.aceeb1411ax013fx57f9xd00cx00000670c5f00.e4(Unknown Source)
//                	at org.apache.derby.impl.services.reflect.DirectCall.invoke(Unknown Source)
//                	at org.apache.derby.impl.sql.execute.ProjectRestrictResultSet.getNextRowCore(Unknown Source)
//                	at org.apache.derby.impl.sql.execute.ProjectRestrictResultSet.getNextRowCore(Unknown Source)
//                	at org.apache.derby.impl.sql.execute.SortResultSet.getRowFromResultSet(Unknown Source)
//                	at org.apache.derby.impl.sql.execute.SortResultSet.getNextRowFromRS(Unknown Source)
//                	at org.apache.derby.impl.sql.execute.SortResultSet.loadSorter(Unknown Source)
//                	at org.apache.derby.impl.sql.execute.SortResultSet.openCore(Unknown Source)
//                	at org.apache.derby.impl.sql.execute.BasicNoPutResultSetImpl.open(Unknown Source)
//                	at org.apache.derby.impl.sql.GenericPreparedStatement.executeStmt(Unknown Source)
//                	at org.apache.derby.impl.sql.GenericPreparedStatement.execute(Unknown Source)
            expectSQLException(e, "derby");
        }
    }

    public void testAsElseArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.eq("Bill").then(employee.firstName)
                .orElse(createWhenClause(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make((String)null, "Cooper"),
                Pair.make("James", "First"),
                Pair.make("Bill", "March"),
                Pair.make((String)null, "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    public void testAsSortSpec() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClause(employee), employee.lastName)
                .list(getDatabaseGate());
        // order is unspecified;
        // assume NULLS LAST by default
        // first sort field is "James", "Margaret", NULL, NULL, NULL,
        try {
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (AssertionFailedError e) {
            // mysql: default is NULLS FIRST
            if ("mysql".equals(getDatabaseName())) {
                assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
            } else {
                throw e;
            }
        }
    }

    public void testNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClause(employee).nullsFirst(), employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClause(employee).nullsLast(), employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS LAST:
                // You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'NULLS LAST
            expectSQLException(e, "mysql");
        }
    }

    public void testAsc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClause(employee).asc(), employee.lastName)
                .list(getDatabaseGate());
        // order is unspecified;
        // assume NULLS LAST by default
        try {
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (AssertionFailedError e) {
            // mysql: default is NULLS FIRST
            if ("mysql".equals(getDatabaseName())) {
                assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
            } else {
                throw e;
            }
        }
    }

    public void testDesc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClause(employee).desc(), employee.lastName)
                .list(getDatabaseGate());
        // order is unspecified;
        // assume NULLS to be at the beginning for DESC
        try {
            assertEquals(Arrays.asList("Cooper","March", "Pedersen", "Redwood", "First"), list);
        } catch (AssertionFailedError e) {
            // mysql: default is NULLS FIRST, reversed for DESC
            if ("mysql".equals(getDatabaseName())) {
                assertEquals(Arrays.asList("Redwood", "First", "Cooper", "March", "Pedersen"), list);
            } else {
                throw e;
            }
        }
    }

    // TODO: derby objects if the argument of second THEN is ?
    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = whenClause.opposite().pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(-3000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(-3000.0, "Redwood")
        ), list);
    }

    public void testAdd() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Number, String>> list = whenClause.add(100.0).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(3100.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(3100.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    public void testSub() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Number, String>> list = whenClause.sub(100.0).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(2900.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(2900.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    public void testMult() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Number, String>> list = whenClause.mult(2).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    public void testDiv() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Number, String>> list = whenClause.div(3).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(1000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(1000.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    private List<Pair<Double, String>> convertToDoubleStringPairList(List<Pair<Number, String>> source) {
        final List<Pair<Double, String>> result = new ArrayList<Pair<Double, String>>();
        for (Pair<Number, String> p : source) {
            result.add(Pair.make(p.first() == null ? null : p.first().doubleValue(), p.second()));
        }
        return result;
    }

    public void testConcat() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClause(employee).concat("+")
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make((String) null, "Cooper"),
                Pair.make("James+", "First"),
                Pair.make((String) null, "March"),
                Pair.make((String) null, "Pedersen"),
                Pair.make("Margaret+", "Redwood")
        ), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = whenClause.unionAll(new Employee().firstName).list(getDatabaseGate());
        assertEquals(Arrays.asList(
                "(null)", "(null)", "(null)", "Alex", "Bill", "James", "James", "James", "Margaret", "Margaret"
        ), replaceNullsAndSort(list));
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = whenClause.unionDistinct(new Employee().firstName).list(getDatabaseGate());
        assertEquals(Arrays.asList(
                "(null)", "Alex", "Bill", "James", "Margaret"
        ), replaceNullsAndSort(list));
    }


    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = whenClause.union(new Employee().firstName).list(getDatabaseGate());
        assertEquals(Arrays.asList(
                "(null)", "Alex", "Bill", "James", "Margaret"
        ), replaceNullsAndSort(list));
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.exceptAll(new Employee().firstName).list(getDatabaseGate());
            assertEquals(Arrays.asList(
                    "(null)", "(null)", "(null)"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.exceptDistinct(new Employee().firstName).list(getDatabaseGate());
            assertEquals(Arrays.asList(
                    "(null)"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.except(new Employee().firstName).list(getDatabaseGate());
            assertEquals(Arrays.asList(
                    "(null)"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.intersectAll(new Employee().firstName).list(getDatabaseGate());
            assertEquals(Arrays.asList(
                    "James", "Margaret"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.intersectDistinct(new Employee().firstName).list(getDatabaseGate());
            assertEquals(Arrays.asList(
                    "James", "Margaret"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.intersect(new Employee().firstName).list(getDatabaseGate());
            assertEquals(Arrays.asList(
                    "James", "Margaret"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "mysql");
        }
    }

    public void testCount() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClause(employee).count().list(getDatabaseGate());
        // only NOT NULL values are counted
        assertEquals(Arrays.asList(2), list);
    }

    public void testCountDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClause(employee).count().list(getDatabaseGate());
        assertEquals(Arrays.asList(2), list);
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee).min().list(getDatabaseGate());
        assertEquals(Arrays.asList("James"), list);
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee).max().list(getDatabaseGate());
        assertEquals(Arrays.asList("Margaret"), list);
    }

    public void testSum() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Number> list = whenClause.sum().list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(6000.0, list.get(0).doubleValue());

    }

    private AbstractSearchedWhenClause<Double> createNumericWC(final Employee employee) {
        return employee.empId.eq(employee.department().manager().empId).then(employee.salary);
    }

    public void testAvg() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Number> list = whenClause.avg().list(getDatabaseGate());
        assertEquals(1, list.size());
        // average is claculated over NOT NULL values only
        assertEquals(3000.0, list.get(0).doubleValue());
    }
}
