package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.sql.AbstractSearchedWhenClauseList;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Label;
import org.symqle.sql.Mappers;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractSearchedWhenClauseListTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class WhenClauseListTest extends AbstractIntegrationTestBase implements AbstractSearchedWhenClauseListTestSet {

    private AbstractSearchedWhenClauseList<String> createWhenClauseList(final Employee employee) {
        return employee.salary.gt(2500.0).then(Params.p("high")).orWhen(employee.salary.lt(1800.0).then(Params.p("low"))).orElse(employee.firstName);
    }

    private AbstractSearchedWhenClauseList<Double> createNumericWCL(final Employee employee) {
        return employee.empId.eq(employee.department().manager().empId).then(employee.salary).orWhen(employee.retired.asBoolean().then(employee.salary.opposite())).orElse(employee.department().manager().salary);
    }

    private AbstractSearchedWhenClauseList<String> createNamesWCL(final Employee employee) {
        return employee.salary.gt(2500.0).then(employee.lastName)
                .orWhen(employee.salary.lt(1800.0).then(employee.firstName)).orElse(Params.p("Nobody"));
    }

    @Override
    public void test_add_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList.add(1000).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-500.0, "Cooper"),
                Pair.make(4000.0, "First"),
                Pair.make(4000.0, "March"),
                Pair.make(4000.0, "Pedersen"),
                Pair.make(4000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = employee.salary.add(whenClauseList).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(0.0, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make(5000.0, "March"),
                Pair.make(5000.0, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_add_Term() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList.add(employee.salary).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(0.0, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make(5000.0, "March"),
                Pair.make(5000.0, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<String> list = employee.lastName.where(employee.salary.add(1000).map(Mappers.DOUBLE)
                .in(employee.salary.mult(2).sub(500).map(Mappers.DOUBLE).asInValueList().append(whenClauseList)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<String> list = employee.lastName.where(employee.salary.add(1000).map(Mappers.DOUBLE)
                .in(whenClauseList.asInValueList().append(employee.salary.mult(2).sub(500).map(Mappers.DOUBLE))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_asBoolean_() throws Exception {
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
            final AbstractSearchedWhenClauseList<Integer> whenClauseBaseList =
                    insertTable.text.eq("abc").then(insertTable.payload)
                    .orWhen(insertTable.text.eq("def").then(insertTable.payload.sub(2).map(Mappers.INTEGER)))
                    .orElse(insertTable.payload.sub(3).map(Mappers.INTEGER));
            final List<String> list = insertTable.text.where(whenClauseBaseList.asBoolean()).list(getEngine());
            // "abc" -> 1 -> true
            // "def" -> 2 - 2 = 0 -> false
            // "xyz" -> 3 - 3 = 0 -> false
            assertEquals(Arrays.asList("abc"), list);
        } catch (SQLException e) {
            // ERROR 42846: Cannot convert types 'INTEGER' to 'BOOLEAN'
            // org.postgresql.util.PSQLException: ERROR: cannot cast type numeric to boolean
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseList(employee).asc(), employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Pedersen", "March", "First", "Redwood", "Cooper"), list);
    }

    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Number> list = whenClauseList.avg().list(getEngine());
        assertEquals(1, list.size());
        // average is calculated over NOT NULL values only
        assertEquals(2100.0, list.get(0).doubleValue());
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final Label label = new Label();
        final List<String> list = createWhenClauseList(employee)
                .cast("CHAR(4)")
                .label(label)
                .orderBy(label)
                .list(getEngine());
        final List<String> expected;
        if (NO_PADDING_ON_CAST_TO_CHAR.contains(getDatabaseName())) {
            expected = Arrays.asList("Alex", "Bill", "high", "high", "low");
        } else {
            expected = Arrays.asList("Alex", "Bill", "high", "high", "low ");
        }
        assertEquals(expected, list);
    }

    @Override
    public void test_charLength_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClauseList(employee).charLength().where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(Arrays.asList(3), list);
    }

    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();
        try {
            Label l = new Label();
            final List<String> list = createWhenClauseList(employee)
                    .collate(validCollationNameForVarchar())
                    .concat("+").label(l)
                    .orderBy(l)
                    .list(getEngine());
            assertEquals(Arrays.asList(
                    "Alex+",
                    "Bill+",
                    "high+",
                    "high+",
                    "low+"
            ), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE"
            // org.h2.jdbc.JdbcSQLException: Syntax error in SQL statement
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .compileQuery(getEngine()).list();
        assertTrue(list.toString(), list.remove("Alex"));
        assertTrue(list.toString(), list.remove("Bill"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClauseList(employee).concat(employee.firstName)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("lowJames", "Cooper"),
                Pair.make("highJames", "First"),
                Pair.make("BillBill", "March"),
                Pair.make("AlexAlex", "Pedersen"),
                Pair.make("highMargaret", "Redwood")
        ), list);
    }

    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClauseList(employee).concat("+")
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("low+", "Cooper"),
                Pair.make("high+", "First"),
                Pair.make("Bill+", "March"),
                Pair.make("Alex+", "Pedersen"),
                Pair.make("high+", "Redwood")
        ), list);
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.concat(createWhenClauseList(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Jameslow", "Cooper"),
                Pair.make("Jameshigh", "First"),
                Pair.make("BillBill", "March"),
                Pair.make("AlexAlex", "Pedersen"),
                Pair.make("Margarethigh", "Redwood")
        ), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // if employee is in current department, than that employee name
        // else if employee does not belong to any department, then current dept manager name
        // else employee first name
        // for "HR" department it is "Redwood", "James", "March", "Alex", "Redwood"
        // for "DEV" department : "First", "First, "Bill", "Pedersen", "Margaret"
        final AbstractSearchedWhenClauseList<String> whenClauseBaseList =
                employee.deptId.eq(department.deptId).then(employee.lastName)
                        .orWhen(employee.deptId.isNull().then(department.manager().lastName))
                        .orElse(employee.firstName);
        final List<String> list = department.deptName.where(whenClauseBaseList.contains("Redwood"))
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClauseList(employee).countDistinct().list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClauseList(employee).count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseList(employee).desc(), employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood", "March", "Pedersen"), list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .distinct().list(getEngine());
        assertTrue(list.toString(), list.remove("Alex"));
        assertTrue(list.toString(), list.remove("Bill"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_div_Factor() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList.div(employee.salary)
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-1.0, "Cooper"),
                Pair.make(1.0, "First"),
                Pair.make(1.5, "March"),
                Pair.make(1.5, "Pedersen"),
                Pair.make(1.0, "Redwood")
        ), list);
    }

    @Override
    public void test_div_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList.div(3).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-500.0, "Cooper"),
                Pair.make(1000.0, "First"),
                Pair.make(1000.0, "March"),
                Pair.make(1000.0, "Pedersen"),
                Pair.make(1000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = employee.salary.mult(3).div(whenClauseList)
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-3.0, "Cooper"),
                Pair.make(3.0, "First"),
                Pair.make(2.0, "March"),
                Pair.make(2.0, "Pedersen"),
                Pair.make(3.0, "Redwood")
        ), list);
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).eq("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_eq_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).eq(employee.firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq(createWhenClauseList(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = new Employee().firstName.exceptAll(whenClauseList).list(getEngine());
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.exceptAll(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Nobody"));
            assertTrue(list.toString(), list.remove("Nobody"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = new Employee().firstName.exceptDistinct(whenClauseList).list(getEngine());
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.exceptDistinct(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Nobody"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = new Employee().firstName.except(whenClauseList).list(getEngine());
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.except(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Nobody"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
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
        final AbstractSearchedWhenClauseList<Integer> whenClauseBaseList =
                employee.deptId.eq(department.deptId).and(employee.firstName.eq("James")).then(department.deptId)
                        .orWhen(employee.deptId.isNull().then(employee.deptId))
                        .orElse(employee.deptId.opposite());
        final List<String> list = department.deptName.where(whenClauseBaseList.exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .forReadOnly().list(getEngine());
        assertTrue(list.toString(), list.remove("Alex"));
        assertTrue(list.toString(), list.remove("Bill"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .forUpdate().list(getEngine());
        assertTrue(list.toString(), list.remove("Alex"));
        assertTrue(list.toString(), list.remove("Bill"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).ge("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).ge(employee.firstName.substring(2)))
                .orderBy(employee.lastName)
                .list(getEngine());
        // "ooper" >= "low", "high" >= "ames", "high" >= "argaret"
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.substring(2).ge(createWhenClauseList(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        // "ooper" >= "low", "high" >= "ames", "high" >= "argaret"
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).gt("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_gt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).gt(employee.firstName.substring(2)))
                .orderBy(employee.lastName)
                .list(getEngine());
        // "ooper" >= "low", "high" >= "ames", "high" >= "argaret"
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_gt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.substring(2).gt(createWhenClauseList(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        // "ooper" >= "low", "high" >= "ames", "high" >= "argaret"
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = employee.lastName.where(whenClauseList.in(james.lastName.where(james.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }


    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).in("high", (String) null))
                .orderBy(employee.lastName)
                .list(getEngine());
        // NULLs should not match to (String)null
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee target = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = target.lastName.where(target.lastName.in(whenClauseList))
                .orderBy(target.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Employee employee = new Employee();
        final Employee target = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        // James, First, Redwood, Nobody, Nobody
        final List<String> list = target.lastName.where(target.lastName.lt(whenClauseList.all()))
                .orderBy(target.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_any_() throws Exception {
        final Employee employee = new Employee();
        final Employee target = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        // James, First, Redwood, Nobody, Nobody
        final List<String> list = target.lastName.where(target.lastName.ne(whenClauseList.any()))
                .orderBy(target.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Employee employee = new Employee();
        final Employee target = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        // James, First, Redwood, Nobody, Nobody
        final List<String> list = target.lastName.where(target.lastName.eq(whenClauseList.some()))
                .orderBy(target.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }
    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.intersectAll(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = new Employee().lastName.intersectAll(whenClauseList).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.intersectDistinct(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = new Employee().lastName.intersectDistinct(whenClauseList).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = whenClauseList.intersect(new Employee().lastName).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        try {
            final List<String> list = new Employee().lastName.intersect(whenClauseList).list(getEngine());
            assertTrue(list.toString(), list.remove("First"));
            assertTrue(list.toString(), list.remove("Redwood"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = employee.salary.gt(2500.0).then(employee.lastName)
                .orWhen(employee.salary.lt(1800.0).thenNull()).orElse(Params.p("Nobody"));
        final List<String> list = employee.lastName
                .where(whenClauseList.isNotNull())
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                "First",
                "March",
                "Pedersen",
                "Redwood"
        ), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = employee.salary.gt(2500.0).then(employee.lastName)
                .orWhen(employee.salary.lt(1800.0).thenNull()).orElse(Params.p("Nobody"));
        final List<String> list = employee.lastName
                .where(whenClauseList.isNull())
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                "Cooper"
        ), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final Label label = new Label();
        final List<String> list = whenClauseList.label(label)
                .orderBy(label).list(getEngine());
        assertEquals(Arrays.asList("First", "James", "Nobody", "Nobody", "Redwood"), list);
    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).le("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).le(employee.firstName.substring(2)))
                .orderBy(employee.lastName)
                .list(getEngine());
        // "ooper" > "low", "high" > "ames", "high" > "argaret"
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_le_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.substring(2).le(createWhenClauseList(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        // "ooper" >= "low", "high" >= "ames", "high" >= "argaret"
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).like("hi%"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).like(employee.firstName.substring(1,3).concat("%")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .limit(2, 5).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .list(getEngine());
        assertTrue(list.toString(), list.remove("Alex"));
        assertTrue(list.toString(), list.remove("Bill"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_countRows_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClauseList(employee)
                .countRows()
                .list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).lt("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_lt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).lt(employee.firstName.substring(2)))
                .orderBy(employee.lastName)
                .list(getEngine());
        // "ooper" > "low", "high" > "ames", "high" > "argaret"
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.substring(2).lt(createWhenClauseList(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        // "ooper" >= "low", "high" >= "ames", "high" >= "argaret"
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        Label label = new Label();
        final List<Integer> list = createNumericWCL(employee).map(Mappers.INTEGER).label(label)
                .orderBy(label).list(getEngine());
        assertEquals(Arrays.asList(-1500, 3000, 3000, 3000, 3000), list);
    }

    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee).max().list(getEngine());
        assertEquals(Arrays.asList("low"), list);
    }

    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee).min().list(getEngine());
        assertEquals(Arrays.asList("Alex"), list);
    }

    @Override
    public void test_mult_Factor() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList.mult(employee.salary.div(employee.department().manager().salary)).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(3000.0, "First"),
                Pair.make(2000.0, "March"),
                Pair.make(2000.0, "Pedersen"),
                Pair.make(3000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_mult_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList.mult(2).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-3000.0, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make(6000.0, "March"),
                Pair.make(6000.0, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = employee.salary.div(employee.department().manager().salary).mult(whenClauseList).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(3000.0, "First"),
                Pair.make(2000.0, "March"),
                Pair.make(2000.0, "Pedersen"),
                Pair.make(3000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).ne("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_ne_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).ne(employee.firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.ne(createWhenClauseList(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = employee.lastName.where(whenClauseList.notIn(james.lastName.where(james.firstName.eq("James"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).notIn("high"))
                .orderBy(employee.lastName)
                .list(getEngine());
        // for NULLs NOT IN is false
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee target = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = target.lastName.where(target.lastName.notIn(whenClauseList))
                .orderBy(target.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).notLike("hi%"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClauseList(employee).notLike(employee.firstName.substring(1, 3).concat("%")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_nullsFirst_() throws Exception {
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
            final AbstractSearchedWhenClauseList<Integer> whenClauseBaseList =
                    insertTable.text.eq("abc").then(insertTable.payload)
                    .orWhen(insertTable.text.eq("def").thenNull())
                    .orElse(insertTable.payload.opposite());
            final List<String> list = insertTable.text
                    .orderBy(whenClauseBaseList.nullsFirst())
                    .list(getEngine());
            assertEquals(Arrays.asList("def", "xyz", "abc"), list);
        } catch (SQLException e) {
            // MySQL does not support NULLS FIRST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
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
            final AbstractSearchedWhenClauseList<Integer> whenClauseBaseList =
                    insertTable.text.eq("abc").then(insertTable.payload)
                    .orWhen(insertTable.text.eq("def").thenNull())
                    .orElse(insertTable.payload.opposite());
            final List<String> list = insertTable.text
                    .orderBy(whenClauseBaseList.nullsLast())
                    .list(getEngine());
            assertEquals(Arrays.asList("xyz", "abc", "def"), list);
        } catch (SQLException e) {
            // MySQL does not support NULLS FIRST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList.opposite()
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(1500.0, "Cooper"),
                Pair.make(-3000.0, "First"),
                Pair.make(-3000.0, "March"),
                Pair.make(-3000.0, "Pedersen"),
                Pair.make(-3000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = employee.lastName.eq("March").then(employee.salary).orElse(whenClauseList)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-1500.0, "Cooper"),
                Pair.make(3000.0, "First"),
                Pair.make(2000.0, "March"),
                Pair.make(3000.0, "Pedersen"),
                Pair.make(3000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(createWhenClauseList(employee), employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Pedersen", "March", "First", "Redwood", "Cooper"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee).orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("low", "high", "Bill", "Alex", "high"), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-1500.0, "Cooper"),
                Pair.make(3000.0, "First"),
                Pair.make(3000.0, "March"),
                Pair.make(3000.0, "Pedersen"),
                Pair.make(3000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<String, Double>> list = employee.lastName
                .pair(whenClauseList)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", -1500.0),
                Pair.make("First", 3000.0),
                Pair.make("March", 3000.0),
                Pair.make("Pedersen", 3000.0),
                Pair.make("Redwood", 3000.0)
        ), list);
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final DynamicParameter<Double> param = whenClauseList.param();
        param.setValue(-1500.0);
        final List<String> list = employee.lastName
                .where(whenClauseList.eq(param))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final DynamicParameter<Double> param = whenClauseList.param(-1500.0);
        final List<String> list = employee.lastName
                .where(whenClauseList.eq(param))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_positionOf_String() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Integer, String>> list = createWhenClauseList(employee).positionOf("i")
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(0, "Cooper"),
                Pair.make(2, "First"),
                Pair.make(2, "March"),
                Pair.make(0, "Pedersen"),
                Pair.make(2, "Redwood")
        ), list);
    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Integer, String>> list = createWhenClauseList(employee).positionOf(employee.firstName)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(0, "Cooper"),
                Pair.make(0, "First"),
                Pair.make(1, "March"),
                Pair.make(1, "Pedersen"),
                Pair.make(0, "Redwood")
        ), list);
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Integer, String>> list = employee.firstName.positionOf(createWhenClauseList(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(0, "Cooper"),
                Pair.make(0, "First"),
                Pair.make(1, "March"),
                Pair.make(1, "Pedersen"),
                Pair.make(0, "Redwood")
        ), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final MyDual myDual = new MyDual();
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList =
                myDual.dummy.eq("X").then(myDual.dummy).orWhen(myDual.dummy.eq("Y").thenNull()).orElse(myDual.dummy.concat("?"));
        final List<Pair<String, String>> list = employee.lastName.pair(whenClauseList.queryValue()).
                where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("Cooper", "X"), list.get(0));
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String>  expected = new ArrayList<>(Arrays.asList("Alex", "Bill", "high", "high", "low"));
        createWhenClauseList(employee)
                .scroll(getEngine(), new Callback<String>() {
                    @Override
                    public boolean iterate(final String s) throws SQLException {
                        assertTrue(expected.toString(), expected.remove(s));
                        return true;
                    }
                });
        assertTrue(expected.toString(), expected.isEmpty());
    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .selectAll().list(getEngine());
        assertTrue(list.toString(), list.remove("Alex"));
        assertTrue(list.toString(), list.remove("Bill"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("high"));
        assertTrue(list.toString(), list.remove("low"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
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
        final AbstractSearchedWhenClauseList<Integer> whenClauseList =
                insertTable.text.eq("abc").then(insertTable.payload)
                .orWhen(insertTable.text.eq("def").thenNull())
                .orElse(insertTable.payload.opposite());
        insertTable.update(insertTable.payload.set(whenClauseList)).execute(getEngine());
        final List<Pair<String, Integer>> list = insertTable.text.pair(insertTable.payload)
                .orderBy(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("abc", 1),
                Pair.make("def", (Integer)null),
                Pair.make("xyz", -3)),
                list);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = createWhenClauseList(employee).showQuery(getEngine().getDialect());
        final Pattern expected;
        expected = Pattern.compile("SELECT CASE WHEN ([A-Z][A-Z0-9]*)\\.salary > \\? THEN \\? WHEN \\1\\.salary < \\? THEN \\? ELSE \\1\\.first_name END AS [A-Z][A-Z0-9]* FROM employee AS \\1");
        assertTrue(sql, expected.matcher(sql).matches());
    }

    @Override
    public void test_sub_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList.sub(100.0).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-1600.0, "Cooper"),
                Pair.make(2900.0, "First"),
                Pair.make(2900.0, "March"),
                Pair.make(2900.0, "Pedersen"),
                Pair.make(2900.0, "Redwood")
        ), list);
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = employee.salary.sub(whenClauseList).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(3000.0, "Cooper"),
                Pair.make(0.0, "First"),
                Pair.make(-1000.0, "March"),
                Pair.make(-1000.0, "Pedersen"),
                Pair.make(0.0, "Redwood")
        ), list);
    }

    @Override
    public void test_sub_Term() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = whenClauseList.sub(employee.salary).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-3000.0, "Cooper"),
                Pair.make(0.0, "First"),
                Pair.make(1000.0, "March"),
                Pair.make(1000.0, "Pedersen"),
                Pair.make(0.0, "Redwood")
        ), list);
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        final Employee employee = new Employee();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(2)
                        .also(insertTable.text.set("abc")))).execute(getEngine());
        final AbstractSearchedWhenClauseList<String> whenClauseList = createWhenClauseList(employee);
        final List<Pair<String, String>> list = whenClauseList.substring(insertTable.id.queryValue())
                .pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("ow", "Cooper"),
                Pair.make("igh", "First"),
                Pair.make("ill", "March"),
                Pair.make("lex", "Pedersen"),
                Pair.make("igh", "Redwood")
        ), list);
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        final Employee employee = new Employee();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(2)
                        .also(insertTable.text.set("abc")))).execute(getEngine());
        final AbstractSearchedWhenClauseList<String> whenClauseList = createWhenClauseList(employee);
        final List<Pair<String, String>> list = whenClauseList
                .substring(insertTable.id.queryValue(), insertTable.payload.queryValue())
                .pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("ow", "Cooper"),
                Pair.make("ig", "First"),
                Pair.make("il", "March"),
                Pair.make("le", "Pedersen"),
                Pair.make("ig", "Redwood")
        ), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("abc")))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("def")))).execute(getEngine());
        insertTable.insert(insertTable.id.set(3)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("pqrstuv")))).execute(getEngine());
        final AbstractSearchedWhenClauseList<Integer> whenClauseList =
                insertTable.text.eq("abc").then(insertTable.payload)
                        .orWhen(insertTable.text.eq("def").then(insertTable.id)).orElse(Params.p(4));
        final List<String> list = insertTable.text.substring(whenClauseList)
                .orderBy(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList("c", "ef", "stuv"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("abc01")))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("def23")))).execute(getEngine());
        insertTable.insert(insertTable.id.set(3)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("pqrstuv")))).execute(getEngine());
        final AbstractSearchedWhenClauseList<Integer> whenClauseList =
                insertTable.text.eq("abc01").then(insertTable.payload)
                        .orWhen(insertTable.text.eq("def23").then(insertTable.id)).orElse(Params.p(4));
        final List<String> list = insertTable.text.substring(whenClauseList, insertTable.payload)
                .orderBy(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList("c01", "ef2", "stu"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("abc01")))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("def23")))).execute(getEngine());
        insertTable.insert(insertTable.id.set(3)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("pqrstuv")))).execute(getEngine());
        final AbstractSearchedWhenClauseList<Integer> whenClauseList =
                insertTable.text.eq("abc01").then(insertTable.payload)
                        .orWhen(insertTable.text.eq("def23").then(insertTable.id)).orElse(Params.p(4));
        final List<String> list = insertTable.text.substring(insertTable.payload, whenClauseList)
                .orderBy(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList("c01", "f2", "rstu"), list);
    }

    @Override
    public void test_substring_int() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createWhenClauseList(employee);
        final List<Pair<String, String>> list = whenClauseList.substring(2)
                .pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("ow", "Cooper"),
                Pair.make("igh", "First"),
                Pair.make("ill", "March"),
                Pair.make("lex", "Pedersen"),
                Pair.make("igh", "Redwood")
        ), list);
    }

    @Override
    public void test_substring_int_int() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createWhenClauseList(employee);
        final List<Pair<String, String>> list = whenClauseList.substring(2, 2)
                .pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("ow", "Cooper"),
                Pair.make("ig", "First"),
                Pair.make("il", "March"),
                Pair.make("le", "Pedersen"),
                Pair.make("ig", "Redwood")
        ), list);
    }

    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Number> list = whenClauseList.sum().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(10500.0, list.get(0).doubleValue());
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<Double> whenClauseList = createNumericWCL(employee);
        final List<Pair<Double, String>> list = employee.lastName.ne("March").then(whenClauseList).orElse(employee.salary)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(-1500.0, "Cooper"),
                Pair.make(3000.0, "First"),
                Pair.make(2000.0, "March"),
                Pair.make(3000.0, "Pedersen"),
                Pair.make(3000.0, "Redwood")
        ), list);
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = new Employee().lastName.unionAll(whenClauseList).list(getEngine());
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
        assertTrue(list.toString(), list.remove("Redwood"));
        assertTrue(list.toString(), list.remove("Nobody"));
        assertTrue(list.toString(), list.remove("Nobody"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = whenClauseList.unionAll(new Employee().lastName).list(getEngine());
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
        assertTrue(list.toString(), list.remove("Redwood"));
        assertTrue(list.toString(), list.remove("Nobody"));
        assertTrue(list.toString(), list.remove("Nobody"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = new Employee().lastName.unionDistinct(whenClauseList).list(getEngine());
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
        assertTrue(list.toString(), list.remove("Nobody"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = whenClauseList.unionDistinct(new Employee().lastName).list(getEngine());
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
        assertTrue(list.toString(), list.remove("Nobody"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = new Employee().lastName.unionDistinct(whenClauseList).list(getEngine());
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
        assertTrue(list.toString(), list.remove("Nobody"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClauseList<String> whenClauseList = createNamesWCL(employee);
        final List<String> list = whenClauseList.unionDistinct(new Employee().lastName).list(getEngine());
        assertTrue(list.toString(), list.remove("Cooper"));
        assertTrue(list.toString(), list.remove("First"));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("March"));
        assertTrue(list.toString(), list.remove("Pedersen"));
        assertTrue(list.toString(), list.remove("Redwood"));
        assertTrue(list.toString(), list.remove("Nobody"));
        assertTrue(list.toString(), list.isEmpty());
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClauseList(employee)
                .where(employee.firstName.eq("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                "low",
                "high"
        ), list);
    }

}
