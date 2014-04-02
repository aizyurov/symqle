package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.sql.AbstractPredicate;
import org.symqle.sql.Params;
import org.symqle.sql.Predicate;
import org.symqle.testset.AbstractPredicateTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class ComparisonPredicateTest extends AbstractIntegrationTestBase implements AbstractPredicateTestSet {

    /**
     * Returns condition, which is true for ["Cooper"]
     * @param employee
     * @return
     */
    private AbstractPredicate createPredicate(final Employee employee) {
        return employee.lastName.eq("Cooper");
    }

    @Override
    public void test_adapt_Predicate() throws Exception {
        final Employee employee = new Employee();
        final Predicate predicate = createPredicate(employee);
        final List<String> list = employee.lastName.where(AbstractPredicate.adapt(predicate).or(employee.lastName.eq("Redwood")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "Redwood"), list);
    }

    @Override
    public void test_and_BooleanFactor() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        final List<String> list = employee.lastName.where(predicate.and(employee.salary.gt(2500.0))).list(getEngine());
        assertEquals(0, list.size());
    }

    @Override
    public void test_and_BooleanTerm_BooleanFactor_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        final List<String> list = employee.lastName.where(employee.salary.gt(2500.0).and(predicate)).list(getEngine());
        assertEquals(0, list.size());
    }

    @Override
    public void test_asValue_() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Boolean>> list = employee.lastName.pair(createPredicate(employee).asValue())
                .where(employee.firstName.eq("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", true),
                Pair.make("First", false)
        ), list);
    }

    @Override
    public void test_isFalse_() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        try {
            final List<String> list = employee.lastName.where(predicate.isFalse())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "FALSE" at line 1, column 92
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_isNotFalse_() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        try {
            final List<String> list = employee.lastName.where(predicate.isNotFalse())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_isNotTrue_() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        try {
            final List<String> list = employee.lastName.where(predicate.isNotTrue())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 96.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_isNotUnknown_() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        try {
            final List<String> list = employee.lastName.where(predicate.isNotUnknown())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(5, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "UNKNOWN" at line 1, column 96.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_isTrue_() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        try {
            final List<String> list = employee.lastName.where(predicate.isTrue())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_isUnknown_() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        try {
            final List<String> list = employee.lastName.where(predicate.isUnknown())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "UNKNOWN" at line 1, column 92.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_negate_() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        final List<String> list = employee.lastName.where(predicate.negate()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_or_BooleanExpression_BooleanTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        final List<String> list = employee.lastName.where(predicate.or(employee.firstName.eq("Bill")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March"), list);
    }

    @Override
    public void test_or_BooleanTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        final List<String> list = employee.lastName.where(employee.firstName.eq("Bill").or(predicate))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March"), list);
    }

    @Override
    public void test_thenNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        final List<Pair<String,String>> list = employee.lastName.pair(
                    employee.salary.gt(2500.0).then(employee.firstName).orWhen(predicate.thenNull()).orElse(Params.p(":)"))
                )
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("Cooper", null), Pair.make("First", "James"), Pair.make("March", ":)"), Pair.make("Pedersen", ":)"), Pair.make("Redwood", "Margaret")), list);
    }

    @Override
    public void test_then_ValueExpression() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        final List<Pair<String,String>> list = employee.lastName.pair(predicate.then(employee.firstName))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("Cooper", "James"), Pair.make("First", null), Pair.make("March", null), Pair.make("Pedersen", null), Pair.make("Redwood", null)), list);
    }

    @Override
    public void test_where_AggregateQueryBase_WhereClause_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        final List<Integer> list = employee.lastName.count().where(predicate)
                .list(getEngine());
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_where_DeleteStatementBase_WhereClause_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two"))).execute(getEngine());
        assertEquals(1, insertTable.delete().where(insertTable.text.eq("two")).execute(getEngine()));
        assertEquals(Arrays.asList("one"), insertTable.text.list(getEngine()));
    }

    @Override
    public void test_where_QueryBaseScalar_WhereClause_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        final List<String> list = employee.lastName.where(predicate)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_where_QueryBase_WhereClause_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate predicate = createPredicate(employee);
        final List<Pair<String, String>> list = employee.firstName.pair(employee.lastName).where(predicate)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make("James", "Cooper")), list);
    }

    @Override
    public void test_where_UpdateStatementBase_WhereClause_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two"))).execute(getEngine());
        assertEquals(1, insertTable.update(insertTable.text.set("zero")).where(insertTable.text.eq("two")).execute(getEngine()));
        assertEquals(Arrays.asList("one", "zero"), insertTable.text.orderBy(insertTable.text).list(getEngine()));
    }

}
