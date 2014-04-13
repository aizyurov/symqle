package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.sql.AbstractLikePredicateBase;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractLikePredicateBaseTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class LikePredicateBaseTest extends AbstractIntegrationTestBase implements AbstractLikePredicateBaseTestSet {

    /**
     * Returns condition, which is true for ["Cooper", "First"]
     * @param employee
     * @return
     */
    private AbstractLikePredicateBase createBasicCondition(final Employee employee) {
        return employee.firstName.like("%es");
    }

    @Override
    public void test_and_BooleanFactor() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.and(employee.salary.gt(2500.0))).list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_and_BooleanTerm_BooleanFactor_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(employee.salary.gt(2500.0).and(basicCondition)).list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_asValue_() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Boolean>> list = employee.lastName.pair(createBasicCondition(employee).asValue()).orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", true),
                Pair.make("First", true),
                Pair.make("March", false),
                Pair.make("Pedersen", false),
                Pair.make("Redwood", false)
        ), list);
    }

    @Override
    public void test_escape_StringExpression() throws Exception {
        final MyDual myDual = new MyDual();
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("$"))).execute(getEngine());

        final List<String> list = myDual.dummy.where(Params.p("what\\is\\it").like("%\\it").escape(insertTable.text.queryValue())).list(getEngine());
        assertEquals(1, list.size());
        // '%' has no special meaning when escaped
        final List<String> list2 = myDual.dummy.where(Params.p("what\\is\\it").like("$%it").escape(insertTable.text.queryValue())).list(getEngine());
        assertEquals(0, list2.size());
    }

    @Override
    public void test_escape_char() throws Exception {
        final MyDual myDual = new MyDual();
        final List<String> list = myDual.dummy.where(Params.p("what\\is\\it").like("%\\it").escape('$')).list(getEngine());
        assertEquals(1, list.size());
        // '%' has no special meaning when escaped
        final List<String> list2 = myDual.dummy.where(Params.p("what\\is\\it").like("$%it").escape('$')).list(getEngine());
        assertEquals(0, list2.size());
    }

    @Override
    public void test_isFalse_() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isFalse())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "FALSE" at line 1, column 92
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_isNotFalse_() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotFalse())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_isNotTrue_() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotTrue())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 96.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_isNotUnknown_() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isNotUnknown())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(5, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "UNKNOWN" at line 1, column 96.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_isTrue_() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isTrue())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "TRUE" at line 1, column 92
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_isUnknown_() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        try {
            final List<String> list = employee.lastName.where(basicCondition.isUnknown())
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "UNKNOWN" at line 1, column 92.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_negate_() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.negate()).orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_or_BooleanExpression_BooleanTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(employee.firstName.eq("Bill").or(basicCondition))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March"), list);
    }

    @Override
    public void test_or_BooleanTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        final List<String> list = employee.lastName.where(basicCondition.or(employee.firstName.eq("Bill")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March"), list);
    }

    @Override
    public void test_thenNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        final List<Pair<String,String>> list = employee.lastName.pair(
                    employee.salary.gt(2500.0).then(employee.firstName).orWhen(basicCondition.thenNull()).orElse(Params.p(":)"))
                )
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("Cooper", null), Pair.make("First", "James"), Pair.make("March", ":)"), Pair.make("Pedersen", ":)"), Pair.make("Redwood", "Margaret")), list);
    }

    @Override
    public void test_then_ValueExpression() throws Exception {
        final Employee employee = new Employee();
        final AbstractLikePredicateBase basicCondition = createBasicCondition(employee);
        final List<Pair<String,String>> list = employee.lastName.pair(basicCondition.then(employee.firstName))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("Cooper", "James"), Pair.make("First", "James"), Pair.make("March", null), Pair.make("Pedersen", null), Pair.make("Redwood", null)), list);
    }

    @Override
    public void test_where_AggregateQueryBase_WhereClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.count()
                .where(createBasicCondition(employee))
                .list(getEngine());
        assertEquals(Arrays.asList(2), list);
    }

    @Override
    public void test_where_DeleteStatementBase_WhereClause_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("zero"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        final int affectedRows = insertTable.delete().where(insertTable.text.like("on_")).execute(getEngine());
        assertEquals(1, affectedRows);
        assertEquals(Arrays.asList("zero"), insertTable.text.list(getEngine()));
    }

    @Override
    public void test_where_QueryBaseScalar_WhereClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createBasicCondition(employee))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    @Override
    public void test_where_QueryBase_WhereClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.lastName.pair(employee.firstName)
                .where(createBasicCondition(employee))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", "James"),
                Pair.make("First", "James")), list);
    }

    @Override
    public void test_where_UpdateStatementBase_WhereClause_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("zero"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        final int affectedRows = insertTable.update(insertTable.text.set("ONE")).where(insertTable.text.like("on_")).execute(getEngine());
        assertEquals(1, affectedRows);
        assertEquals(Arrays.asList("ONE", "zero"), insertTable.text.orderBy(insertTable.text).list(getEngine()));
    }

}
