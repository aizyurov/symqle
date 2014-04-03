package org.symqle.integration;

import junit.framework.AssertionFailedError;
import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.sql.AbstractSearchedWhenClauseBaseList;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Label;
import org.symqle.sql.Mappers;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractSearchedWhenClauseBaseListTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class WhenClauseBaseListTest extends AbstractIntegrationTestBase implements AbstractSearchedWhenClauseBaseListTestSet {

    private AbstractSearchedWhenClauseBaseList<String> createWhenClauseBaseList(final Employee employee) {
        return employee.salary.gt(2500.0).then(Params.p("high")).orWhen(employee.salary.lt(1800.0).then(Params.p("low")));
    }

    private AbstractSearchedWhenClauseBaseList<Double> createNumericWCBL(final Employee employee) {
        return employee.empId.eq(employee.department().manager().empId)
                .then(employee.salary)
                .orWhen(employee.retired.asPredicate().then(employee.salary.opposite()));
    }
    @Override
    public void test_add_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = whenClauseBaseList.add(100.0).map(Mappers.DOUBLE).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-1400.0, "Cooper"),
                Pair.make(3100.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(3100.0, "Redwood")
        ), list);
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = employee.salary.add(whenClauseBaseList).map(Mappers.DOUBLE).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(0.0, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_add_Term() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = whenClauseBaseList.add(employee.salary).map(Mappers.DOUBLE).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(0.0, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<String> list = employee.lastName.where(
                employee.salary.opposite()
                        .in(employee.department().manager().salary.opposite().asInValueList()
                                .append(whenClauseBaseList)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<String> list = employee.lastName.where(
                employee.salary.opposite()
                        .in(whenClauseBaseList.asInValueList()
                                .append(employee.department().manager().salary.opposite())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_asPredicate_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1)
                    .also(insertTable.payload.set(1)
                            .also(insertTable.text.set("abc")))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2)
                    .also(insertTable.payload.set(2)
                            .also(insertTable.text.set("def")))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3)
                    .also(insertTable.payload.set(3)
                            .also(insertTable.text.set("xyz")))).execute(getEngine());
            final AbstractSearchedWhenClauseBaseList<Integer> whenClauseBaseList =
                    insertTable.text.eq("abc").then(insertTable.payload)
                    .orWhen(insertTable.text.eq("def").then(insertTable.payload.sub(2).map(Mappers.INTEGER)));
            final List<String> list = insertTable.text.where(whenClauseBaseList.asPredicate()).list(getEngine());
            // "abc" -> 1 -> true
            // "def" -> 2 - 2 = 0 -> false
            // "xyz" -> null -> false
            assertEquals(Arrays.asList("abc"), list);
        } catch (SQLException e) {
            // ERROR 42846: Cannot convert types 'INTEGER' to 'BOOLEAN'
            // org.postgresql.util.PSQLException: ERROR: cannot cast type numeric to boolean
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }

    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseBaseList(employee).asc(), employee.lastName)
                .list(getEngine());
        // order is unspecified;
        // NULLS FIRST / NULLS LAST not specified; database-dependent
        final List<String> expected;
        if (Arrays.asList("MySQL").contains(getDatabaseName())) {
        // NULLS FIRST default
            expected = Arrays.asList("March", "Pedersen", "First", "Redwood", "Cooper");
        } else {
            expected = Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen");
        }
        assertEquals(expected, list);
    }

    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Number> list = whenClauseBaseList.avg().list(getEngine());
        assertEquals(1, list.size());
        // average is claculated over NOT NULL values only
        assertEquals(1500.0, list.get(0).doubleValue());
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .cast("CHAR(4)")
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected;
        if (Arrays.asList("MySQL").contains(getDatabaseName())) {
            // databases, which do not add spaces when casting to CHAR(n)
            expected = Arrays.asList("low", "high", null, null, "high");
        } else {
            expected = Arrays.asList("low ", "high", null, null, "high");
        }
        assertEquals(expected, list);
    }

    @Override
    public void test_charLength_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList = employee.salary.gt(2500.0).then(employee.lastName).orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<Integer> list = whenClauseBaseList
                .charLength()
                .where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        // "James"
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Pair<String, String>> list = createWhenClauseBaseList(employee)
                    .collate(validCollationNameForChar())
                    .concat("+")
                    .pair(employee.lastName)
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList(
                    Pair.make("low+", "Cooper"),
                    Pair.make("high+", "First"),
                    Pair.make((String) null, "March"),
                    Pair.make((String) null, "Pedersen"),
                    Pair.make("high+", "Redwood")
            ), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE"
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .compileQuery(getEngine()).list();
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClauseBaseList(employee).concat(employee.firstName)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("lowJames", "Cooper"),
                Pair.make("highJames", "First"),
                Pair.make((String) null, "March"),
                Pair.make((String) null, "Pedersen"),
                Pair.make("highMargaret", "Redwood")
        ), list);
    }

    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClauseBaseList(employee).concat("+")
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("low+", "Cooper"),
                Pair.make("high+", "First"),
                Pair.make((String) null, "March"),
                Pair.make((String) null, "Pedersen"),
                Pair.make("high+", "Redwood")
        ), list);
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.concat(createWhenClauseBaseList(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Jameslow", "Cooper"),
                Pair.make("Jameshigh", "First"),
                Pair.make((String) null, "March"),
                Pair.make((String) null, "Pedersen"),
                Pair.make("Margarethigh", "Redwood")
        ), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // if employee is in current department, than that employee name
        // else if employee does not belong to any department, then current dept manager name
        // else null
        // for "HR" department it is "Redwood", null, "March",null, "Redwood"
        // for "DEV" department : "First", "First, null, "Pedersen", null
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.deptId.eq(department.deptId).then(employee.lastName)
                        .orWhen(employee.deptId.isNull().then(department.manager().lastName));
        final List<String> list = department.deptName.where(whenClauseBaseList.contains("Redwood"))
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClauseBaseList(employee).count().list(getEngine());
        assertEquals(Arrays.asList(3), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClauseBaseList(employee).count().list(getEngine());
        // only NOT NULL values are counted
        assertEquals(Arrays.asList(3), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseBaseList(employee).desc(), employee.lastName)
                .list(getEngine());
        // NULLS FIRST / NULLS LAST is database-dependent if not specified
        final List<String> expected;
        if (Arrays.asList("MySQL").contains(getDatabaseName())) {
        // NULLS FIRST default
            expected = Arrays.asList("Cooper", "First", "Redwood", "March", "Pedersen");
        } else {
            expected = Arrays.asList("March", "Pedersen", "Cooper", "First", "Redwood");
        }
        assertEquals(expected, list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .distinct().list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_div_Factor() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = whenClauseBaseList.div(employee.salary.sub(1000))
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-3.0, "Cooper"),
                Pair.make(1.5, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(1.5, "Redwood")
        ), list);
    }

    @Override
    public void test_div_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = whenClauseBaseList.div(3)
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-500.0, "Cooper"),
                Pair.make(1000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(1000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = employee.salary.add(3000).div(whenClauseBaseList)
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-3.0, "Cooper"),
                Pair.make(2.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(2.0, "Redwood")
        ), list);
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).eq("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_eq_Predicand() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                .orWhen(employee.salary.lt(1700.0).then(employee.firstName));
        final List<String> list = employee.lastName.where(whenClauseBaseList.eq(employee.lastName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);

    }

    @Override
    public void test_eq_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                .orWhen(employee.salary.lt(1700.0).then(employee.firstName));
        final List<String> list = employee.lastName.where(employee.lastName.eq(whenClauseBaseList))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = new Employee().lastName.exceptAll(whenClauseBaseList).list(getEngine());
            assertTrue(list.toString(), list.remove("Cooper"));
            assertTrue(list.toString(), list.remove("March"));
            assertTrue(list.toString(), list.remove("Pedersen"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.exceptAll(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = new Employee().lastName.exceptDistinct(whenClauseBaseList).list(getEngine());
            assertTrue(list.toString(), list.remove("Cooper"));
            assertTrue(list.toString(), list.remove("March"));
            assertTrue(list.toString(), list.remove("Pedersen"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.exceptDistinct(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = new Employee().lastName.except(whenClauseBaseList).list(getEngine());
            assertTrue(list.toString(), list.remove("Cooper"));
            assertTrue(list.toString(), list.remove("March"));
            assertTrue(list.toString(), list.remove("Pedersen"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.except(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // if employee is in current department, and is "James", than deptId
        // else if employee does not belong to any department, then null
        // else null
        // for "HR" department it is null, null, null, null, null
        // for "DEV" department : null, 2, null, null, null
        // both are non-empty, so both departments are returned
        // TODO better test
        final AbstractSearchedWhenClauseBaseList<Integer> whenClauseBaseList =
                employee.deptId.eq(department.deptId).and(employee.firstName.eq("James")).then(department.deptId)
                        .orWhen(employee.deptId.isNull().then(employee.deptId));
        final List<String> list = department.deptName.where(whenClauseBaseList.exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .forReadOnly().list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .forUpdate().list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).ge("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createNumericWCBL(employee).ge(employee.salary.div(2).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.ge(createNumericWCBL(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).gt("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_gt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createNumericWCBL(employee).gt(employee.salary.div(2).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_gt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.mult(2).map(Mappers.DOUBLE).gt(createNumericWCBL(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = employee.lastName.where(whenClauseBaseList.in(james.lastName.where(james.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).in("high", (String) null))
                .orderBy(employee.lastName)
                .list(getEngine());
        // NULLs should not match to (String)null
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // if employee is in current department, than that employee name
        // else if employee does not belong to any department, then current dept manager name
        // else null
        // for "HR" department it is "Redwood", null, "March",null, "Redwood"
        // for "DEV" department : "First", "First, null, "Pedersen", null
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.deptId.eq(department.deptId).then(employee.lastName)
                        .orWhen(employee.deptId.isNull().then(department.manager().lastName));
        final List<String> list = department.deptName.where(department.manager().lastName.in(whenClauseBaseList))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.intersectAll(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = new Employee().lastName.intersectAll(whenClauseBaseList).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.intersectDistinct(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = new Employee().lastName.intersectDistinct(whenClauseBaseList).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = whenClauseBaseList.intersect(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        try {
            final List<String> list = new Employee().lastName.intersectDistinct(whenClauseBaseList).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).isNotNull())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).isNull())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<String> list = createWhenClauseBaseList(employee)
                .label(l)
                .orderBy(l)
                .list(getEngine());
        // order is unspecified;
        // NULLS FIRST / NULLS LAST not specified; database-dependent
        final List<String> expected;
        if (Arrays.asList("MySQL").contains(getDatabaseName())) {
        // NULLS FIRST default
            expected = Arrays.asList(null, null, "high", "high", "low");
        } else {
            expected = Arrays.asList("high", "high", "low", null, null);
        }
        assertEquals(expected, list);
    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).le("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createNumericWCBL(employee).le(employee.salary.mult(2).map(Mappers.DOUBLE)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.div(2).map(Mappers.DOUBLE).lt(createNumericWCBL(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).like("hi%"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.ge(2500.0).then(employee.lastName)
                .orWhen(employee.salary.lt(1700.0).then(employee.firstName));
        final List<String> list = employee.lastName.where(
                whenClauseBaseList.like(employee.lastName.substring(1,3).concat("%")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testAsLikeArgument() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList = employee.salary.ge(2500.0).then(employee.lastName.substring(1, 3).concat("%"))
                .orWhen(employee.salary.lt(1700.0).then(employee.lastName.substring(1, 5).concat("_")));
        final List<String> list = employee.lastName.where(employee.lastName.like(whenClauseBaseList))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .limit(2, 5).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).lt("i"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_lt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createNumericWCBL(employee).lt(employee.salary))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.mult(0.5).map(Mappers.DOUBLE).lt(createNumericWCBL(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<Long> list = createNumericWCBL(employee).map(Mappers.LONG).list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(-1500L));
        assertTrue(list.toString(), list.remove(3000L));
        assertTrue(list.toString(), list.remove(3000L));
    }

    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee).max().list(getEngine());
        assertEquals(Arrays.asList("low"), list);
    }

    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee).min().list(getEngine());
        assertEquals(Arrays.asList("high"), list);
    }

    @Override
    public void test_mult_Factor() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = whenClauseBaseList.mult(employee.salary.div(1000)).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-2250.0, "Cooper"),
                Pair.make(9000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(9000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_mult_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = whenClauseBaseList.mult(2).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-3000.0, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = employee.salary.div(1000).mult(whenClauseBaseList).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-2250.0, "Cooper"),
                Pair.make(9000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(9000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).ne("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_ne_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createNumericWCBL(employee).ne(employee.salary))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.salary.ne(createNumericWCBL(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = employee.lastName.where(whenClauseBaseList.notIn(james.lastName.where(james.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "Redwood"), list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).notIn("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        // for NULLs NOT IN is false
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // if employee is in current department, than that employee name
        // else if employee does not belong to any department, then current dept manager name
        // else null
        // for "HR" department it is "Redwood", null, "March",null, "Redwood"
        // for "DEV" department : "First", "First, null, "Pedersen", null
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.deptId.eq(department.deptId).then(employee.lastName)
                        .orWhen(employee.deptId.isNull().then(department.manager().lastName));
        final List<String> list = department.deptName.where(department.manager().lastName.notIn(whenClauseBaseList))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(0, list.size());
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseBaseList(employee).notLike("hi%"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.ge(2500.0).then(employee.lastName)
                .orWhen(employee.salary.lt(1700.0).then(employee.firstName));
        final List<String> list = employee.lastName.where(
                whenClauseBaseList.notLike(employee.lastName.substring(1, 3).concat("%")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClauseBaseList(employee).nullsFirst(), employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen", "First", "Redwood", "Cooper"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS FIRST
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClauseBaseList(employee).nullsLast(), employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS LAST:
                // You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'NULLS LAST
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = whenClauseBaseList.opposite().pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(1500.0, "Cooper"),
                Pair.make(-3000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(-3000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_orElse_ElseClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee).orElse(Params.p("medium"))
                .list(getEngine());
        assertTrue(list.toString(), list.remove("medium"));
        assertTrue(list.toString(), list.remove("medium"));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.eq("Bill").then(Params.p("medium")).orElse(createWhenClauseBaseList(employee))
                .list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("medium"));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_orWhen_SearchedWhenClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee).orWhen(employee.firstName.eq("Bill").then(employee.firstName))
                .list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("Bill"));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_orWhen_ThenNullClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee).orWhen(employee.firstName.eq("Bill").thenNull())
                .list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseBaseList(employee), employee.lastName)
                .list(getEngine());
        // order is unspecified;
        // assume NULLS LAST by default
        try {
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (AssertionFailedError e) {
            // mysql: default is NULLS FIRST
            if ("MySQL".equals(getDatabaseName())) {
                assertEquals(Arrays.asList("March", "Pedersen", "First", "Redwood", "Cooper"), list);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                "low",
                "high",
                (String)null,
                (String)null,
                "high"
        ), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> list = createWhenClauseBaseList(employee)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("low", "Cooper"),
                Pair.make("high", "First"),
                Pair.make((String)null, "March"),
                Pair.make((String)null, "Pedersen"),
                Pair.make("high", "Redwood")
        ), list);
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> list = employee.lastName
                .pair(createWhenClauseBaseList(employee))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", "low"),
                Pair.make("First", "high"),
                Pair.make("March", (String)null),
                Pair.make("Pedersen", (String)null),
                Pair.make("Redwood", "high")
        ), list);
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList = createWhenClauseBaseList(employee);
        final DynamicParameter<String> param = whenClauseBaseList.param();
        param.setValue("high");
        final List<String> list = employee.lastName.where(whenClauseBaseList.eq(param))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList = createWhenClauseBaseList(employee);
        final DynamicParameter<String> param = whenClauseBaseList.param("high");
        final List<String> list = employee.lastName.where(whenClauseBaseList.eq(param))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_positionOf_String() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList = createWhenClauseBaseList(employee);
        final List<Pair<Integer, String>> list = whenClauseBaseList.positionOf("ig").pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(0, "Cooper"),
                Pair.make(2, "First"),
                Pair.make((Integer)null, "March"),
                Pair.make((Integer)null, "Pedersen"),
                Pair.make(2, "Redwood")
        ), list);
    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.ge(2500.0).then(employee.lastName)
                .orWhen(employee.salary.lt(1700.0).then(employee.firstName));
        final List<Pair<Integer, String>> list = whenClauseBaseList.positionOf(employee.lastName).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(0, "Cooper"),
                Pair.make(1, "First"),
                Pair.make((Integer)null, "March"),
                Pair.make((Integer)null, "Pedersen"),
                Pair.make(1, "Redwood")
        ), list);
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.ge(2500.0).then(employee.lastName)
                .orWhen(employee.salary.lt(1700.0).then(employee.firstName));
        final List<Pair<Integer, String>> list = employee.lastName.positionOf(whenClauseBaseList).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(0, "Cooper"),
                Pair.make(1, "First"),
                Pair.make((Integer)null, "March"),
                Pair.make((Integer)null, "Pedersen"),
                Pair.make(1, "Redwood")
        ), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final MyDual myDual = new MyDual();
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                myDual.dummy.eq("X").then(myDual.dummy).orWhen(myDual.dummy.eq("Y").thenNull());
        final List<Pair<String, String>> list = employee.lastName.pair(whenClauseBaseList.queryValue()).
                where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("Cooper", "X"), list.get(0));

    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected = new ArrayList<>(Arrays.asList((String)null, (String)null, "low", "high", "high"));
        final int count = createWhenClauseBaseList(employee)
                .scroll(getEngine(), new Callback<String>() {
                    @Override
                    public boolean iterate(final String s) throws SQLException {
                        assertTrue(expected + "does not contain " + s, expected.remove(s));
                        return true;
                    }
                });
        assertEquals(5, count);
    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .selectAll()
                .list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_sub_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Pair<Double, String>> list = whenClauseBaseList.sub(100.0).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-1600.0, "Cooper"),
                Pair.make(2900.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(2900.0, "Redwood")
        ), list);
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_sub_Term() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_substring_int() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_substring_int_int() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<Double> whenClauseBaseList = createNumericWCBL(employee);
        final List<Number> list = whenClauseBaseList.sum().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(4500.0, list.get(0).doubleValue());
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        // TODO implement
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = new Employee().lastName.unionAll(whenClauseBaseList).list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
        assertTrue(list.toString(), list.remove("Redwood"));
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = whenClauseBaseList.unionAll(new Employee().lastName).list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
        assertTrue(list.toString(), list.remove("Redwood"));
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = new Employee().lastName.unionDistinct(whenClauseBaseList).list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = whenClauseBaseList.unionDistinct(new Employee().lastName).list(getEngine());
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = new Employee().lastName.union(whenClauseBaseList).list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseBaseList<String> whenClauseBaseList =
                employee.salary.gt(2500.0).then(employee.lastName)
                        .orWhen(employee.salary.lt(1800.0).then(employee.firstName));
        final List<String> list = whenClauseBaseList.union(new Employee().lastName).list(getEngine());
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseBaseList(employee)
                .where(employee.firstName.eq("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                "low",
                "high"
        ), list);
    }

}
