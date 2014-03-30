package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.InsertTable;
import org.symqle.sql.AbstractBooleanExpression;
import org.symqle.sql.AbstractPredicate;
import org.symqle.sql.Params;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractBooleanFactor;
import org.symqle.testset.AbstractBooleanFactorTestSet;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class BooleanFactorTest extends AbstractIntegrationTestBase implements AbstractBooleanFactorTestSet {

    /**
     * Returns condition, which is true for ["Cooper", "First", "Pedersen"]
     * @param employee
     * @return
     */
    private AbstractBooleanFactor createBooleanFactor(final Employee employee) {
        return employee.hireDate.lt(new Date(108, 9, 1)).negate();
    }

    @Override
    public void test_adapt_BooleanFactor() throws Exception {
        final Employee employee = new Employee();
        final AbstractPredicate abstractPredicate = employee.hireDate.lt(new Date(108, 9, 1));
        final AbstractBooleanFactor adaptor = AbstractBooleanFactor.adapt(abstractPredicate);
        final List<String> list = employee.lastName.where(adaptor)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("March", "Redwood"), list);

    }

    @Override
    public void test_and_BooleanFactor() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor booleanFactor = createBooleanFactor(employee);
        final List<String> list = employee.lastName.where(booleanFactor.and(employee.salary.gt(2500.0))).list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_and_BooleanTerm_BooleanFactor_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor booleanFactor = createBooleanFactor(employee);
        final List<String> list = employee.lastName.where(employee.salary.gt(2500.0).and(booleanFactor)).list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_asValue_() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor booleanFactor = createBooleanFactor(employee);
        final List<Pair<String, Boolean>> list = employee.lastName.pair(booleanFactor.asValue()).
                orderBy(employee.lastName).
                list(getEngine());
        assertEquals(Arrays.asList(
                        Pair.make("Cooper", true),
                        Pair.make("First", true),
                        Pair.make("March", false),
                        Pair.make("Pedersen", true),
                        Pair.make("Redwood", false)),
                        list);
    }

    @Override
    public void test_isFalse_() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isFalse())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "FALSE" at line 1, column 92
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_isNotFalse_() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotFalse())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_isNotTrue_() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotTrue())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 96.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_isNotUnknown_() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotUnknown())
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
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isTrue())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Pedersen"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_isUnknown_() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isUnknown())
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
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        final List<String> list = employee.lastName.where(basicCondition.negate()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("March", "Redwood"), list);
    }

    @Override
    public void test_or_BooleanExpression_BooleanTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        final List<String> list = employee.lastName.where(employee.firstName.eq("Bill").or(basicCondition))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
    }

    @Override
    public void test_or_BooleanTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        final List<String> list = employee.lastName.where(basicCondition.or(employee.firstName.eq("Bill")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
    }

    @Override
    public void test_thenNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        final List<Pair<String,String>> list = employee.lastName.pair(
                    employee.salary.gt(2500.0).then(employee.firstName).orWhen(basicCondition.thenNull()).orElse(Params.p(":)"))
                )
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("Cooper", null), Pair.make("First", "James"), Pair.make("March", ":)"), Pair.make("Pedersen", null), Pair.make("Redwood", "Margaret")), list);
    }

    @Override
    public void test_then_ValueExpression() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor basicCondition = createBooleanFactor(employee);
        final List<Pair<String,String>> list = employee.lastName.pair(basicCondition.then(employee.firstName))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("Cooper", "James"), Pair.make("First", "James"), Pair.make("March", null), Pair.make("Pedersen", "Alex"), Pair.make("Redwood", null)), list);
    }

    @Override
    public void test_where_AggregateQueryBase_WhereClause_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractBooleanFactor booleanFactor = createBooleanFactor(employee);
        final List<Integer> list = employee.empId.count().where(booleanFactor).list(getEngine());
        assertEquals(Arrays.asList(3), list);
    }

    @Override
    public void test_where_DeleteStatementBase_WhereClause_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        prepareTestData(insertTable);
        final AbstractBooleanFactor booleanFactor = insertTable.text.like("t%").negate();
        insertTable.delete().where(booleanFactor).execute(getEngine());
        final List<Integer> list = insertTable.id.orderBy(insertTable.id).list(getEngine());
        assertEquals(Arrays.asList(2,3), list);
    }

    @Override
    public void test_where_QueryBaseScalar_WhereClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createBooleanFactor(employee))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Pedersen"), list);
    }

    @Override
    public void test_where_QueryBase_WhereClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.pair(employee.lastName).where(createBooleanFactor(employee))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make("James", "Cooper"), Pair.make("James", "First"), Pair.make("Alex", "Pedersen")), list);
    }

    @Override
    public void test_where_UpdateStatementBase_WhereClause_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        prepareTestData(insertTable);
        final AbstractBooleanFactor booleanFactor = insertTable.text.like("t%").negate();
        insertTable.update(insertTable.text.set("modified")).where(booleanFactor).execute(getEngine());
        final List<Pair<Integer, String>> list = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(1, "modified"),
                Pair.make(2, "two"),
                Pair.make(3, "three"),
                Pair.make(4, "modified"),
                Pair.make(5, "modified")
                ), list);

    }

    private void prepareTestData(final InsertTable insertTable) throws SQLException {
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("three"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(4).also(insertTable.text.set("four"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(5).also(insertTable.text.set("five"))).execute(getEngine());
    }

}
