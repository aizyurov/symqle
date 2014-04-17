package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.sql.AbstractSearchedWhenClause;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Label;
import org.symqle.sql.Mappers;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractSearchedWhenClauseTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class WhenClauseTest extends AbstractIntegrationTestBase implements AbstractSearchedWhenClauseTestSet {

    private AbstractSearchedWhenClause<String> createWhenClause(final Employee employee) {
        return employee.empId.eq(employee.department().manager().empId).then(employee.firstName);
    }

    private AbstractSearchedWhenClause<Double> createNumericWC(final Employee employee) {
        return employee.empId.eq(employee.department().manager().empId).then(employee.salary);
    }

    /**
     * Test AbstractSearchedWhenClause#add(Number)
     */
    @Override
    public void test_add_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = whenClause.add(100.0)
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(3100.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(3100.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  NumericExpression#add(Term<?>)
     */
    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = employee.salary.div(2).add(whenClause)
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(4500.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(4500.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#add(Term<?>)
     */
    @Override
    public void test_add_Term() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = whenClause.add(employee.salary.div(2))
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(4500.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(4500.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> InValueList#append(ValueExpression<T>)
     */
    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        List<String> list = employee.lastName
                .where(employee.firstName.in(Params.p("Bill").asInValueList().append(whenClause)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#asInValueList()
     */
    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        List<String> list = employee.lastName
                .where(employee.firstName.in(whenClause.asInValueList().append(Params.p("Bill"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#asBoolean()
     */
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
            final AbstractSearchedWhenClause<Integer> whenClauseBaseList =
                    insertTable.text.eq("abc").then(insertTable.payload);;
            final List<String> list = insertTable.text.where(whenClauseBaseList.asBoolean()).list(getEngine());
            // "abc" -> 1 -> true
            // "def" -> null -> false
            // "xyz" -> null -> false
            assertEquals(Arrays.asList("abc"), list);
        } catch (SQLException e) {
            // ERROR 42846: Cannot convert types 'INTEGER' to 'BOOLEAN'
            // org.postgresql.util.PSQLException: ERROR: cannot cast type numeric to boolean
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#asc()
     */
    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            // database(s) with NULLS FIRST default
            expected = Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood");
        } else {
            expected = Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen");
        }
        final List<String> list = employee.lastName.orderBy(createWhenClause(employee).asc(), employee.lastName)
                .list(getEngine());
        assertEquals(expected, list);
    }

    /**
     * Test AbstractSearchedWhenClause#avg()
     */
    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Number> list = whenClause.avg().list(getEngine());
        assertEquals(1, list.size());
        // average is claculated over NOT NULL values only
        assertEquals(3000.0, list.get(0).doubleValue());
    }

    /**
     * Test AbstractSearchedWhenClause#cast(String)
     */
    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .cast("CHAR(8)")
                .list(getEngine());
        final List expected;
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("Margaret"));
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            // no extra spaces added when casted to CHAR
            assertTrue(list.toString(), list.remove("James"));
        } else {
            assertTrue(list.toString(), list.remove("James   "));
        }
        assertTrue(list.toString(), list.isEmpty());
    }

    /**
     * Test AbstractSearchedWhenClause#charLength()
     */
    @Override
    public void test_charLength_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause = employee.salary.gt(2500.0).then(employee.firstName);
        final List<Integer> list = whenClause
                .charLength()
                .where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        // "Margaret"
        assertEquals(Arrays.asList(8), list);
    }

    /**
     * Test AbstractSearchedWhenClause#collate(String)
     */
    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createWhenClause(employee)
                    .collate(validCollationNameForVarchar())
                    .concat("+")
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList(
                    (String) null,
                    "James+",
                    (String) null,
                    (String) null,
                    "Margaret+"
            ), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE"
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#compileQuery(QueryEngine, Option[])
     */
    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .compileQuery(getEngine()).list();
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("Margaret"));
        assertTrue(list.toString(), list.isEmpty());
    }

    /**
     * Test AbstractSearchedWhenClause#concat(CharacterFactor<?>)
     */
    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClause(employee).concat(employee.lastName)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((String) null, "Cooper"),
                Pair.make("JamesFirst", "First"),
                Pair.make((String) null, "March"),
                Pair.make((String) null, "Pedersen"),
                Pair.make("MargaretRedwood", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#concat(String)
     */
    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClause(employee).concat("+")
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((String) null, "Cooper"),
                Pair.make("James+", "First"),
                Pair.make((String) null, "March"),
                Pair.make((String) null, "Pedersen"),
                Pair.make("Margaret+", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  StringExpression#concat(CharacterFactor<?>)
     */
    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.lastName.concat(createWhenClause(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((String) null, "Cooper"),
                Pair.make("FirstJames", "First"),
                Pair.make((String) null, "March"),
                Pair.make((String) null, "Pedersen"),
                Pair.make("RedwoodMargaret", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#contains(T)
     */
    @Override
    public void test_contains_Object() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        // if employee is in current department, than that employee name
        // else null
        // for "HR" department it is null, null, "March",null, "Redwood"
        // for "DEV" department : null, "First, null, "Pedersen", null
        final AbstractSearchedWhenClause<String> whenClause =
                employee.deptId.eq(department.deptId).then(employee.lastName);
        final List<String> list = department.deptName.where(whenClause.contains("Redwood"))
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#countDistinct()
     */
    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClause(employee).count().list(getEngine());
        assertEquals(Arrays.asList(2), list);
    }

    /**
     * Test AbstractSearchedWhenClause#count()
     */
    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = createWhenClause(employee).count().list(getEngine());
        // only NOT NULL values are counted
        assertEquals(Arrays.asList(2), list);
    }

    /**
     * Test AbstractSearchedWhenClause#desc()
     */
    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            // database(s) with NULLS FIRST default - DESC with nulls last
            expected = Arrays.asList("Redwood", "First", "Cooper", "March", "Pedersen");
        } else {
            // DESC means  nulls first
            expected = Arrays.asList("Cooper","March", "Pedersen", "Redwood", "First");
        }
        final List<String> list = employee.lastName.orderBy(createWhenClause(employee).desc(), employee.lastName)
                .list(getEngine());
        assertEquals(expected, list);
    }

    /**
     * Test AbstractSearchedWhenClause#distinct()
     */
    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .distinct().list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("Margaret"));
        assertTrue(list.toString(), list.isEmpty());
    }

    /**
     * Test AbstractSearchedWhenClause#div(Factor<?>)
     */
    @Override
    public void test_div_Factor() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = whenClause.div(employee.salary.sub(2000.0))
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(3.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(3.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#div(Number)
     */
    @Override
    public void test_div_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = whenClause.div(3)
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(1000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(1000.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  Term#div(Factor<?>)
     */
    @Override
    public void test_div_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = employee.salary.add(1500.0)
                .div(whenClause)
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(1.5, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(1.5, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#eq(T)
     */
    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).eq("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#eq(Predicand<T>)
     */
    @Override
    public void test_eq_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(createWhenClause(employee).eq(employee.firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#eq(Predicand<T>)
     */
    @Override
    public void test_eq_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.eq(createWhenClause(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBodyScalar#exceptAll(QueryTerm<T>)
     */
    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = new Employee().firstName.exceptAll(whenClause).list(getEngine());
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#exceptAll(QueryTerm<T>)
     */
    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.exceptAll(new Employee().firstName).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBodyScalar#exceptDistinct(QueryTerm<T>)
     */
    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = new Employee().firstName.exceptDistinct(whenClause).list(getEngine());
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#exceptDistinct(QueryTerm<T>)
     */
    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.exceptDistinct(new Employee().firstName).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBodyScalar#except(QueryTerm<T>)
     */
    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = new Employee().firstName.except(whenClause).list(getEngine());
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#except(QueryTerm<T>)
     */
    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.except(new Employee().firstName).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#exists()
     */
    @Override
    public void test_exists_() throws Exception {
        final Employee pattern = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(pattern);
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(whenClause.exists())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#forReadOnly()
     */
    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .forReadOnly().list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("Margaret"));
        assertTrue(list.toString(), list.isEmpty());
    }

    /**
     * Test AbstractSearchedWhenClause#forUpdate()
     */
    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createWhenClause(employee)
                    .forUpdate().list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            //derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            //org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE cannot be applied to the nullable side of an outer join
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.POSTGRESQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#ge(T)
     */
    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).ge("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#ge(Predicand<T>)
     */
    @Override
    public void test_ge_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<String> list = employee.lastName
                .where(createWhenClause(employee)
                        .ge(cooper.firstName.where(cooper.lastName.eq("Cooper")).queryValue()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#ge(Predicand<T>)
     */
    @Override
    public void test_ge_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<String> list = employee.lastName
                .where(cooper.firstName.where(cooper.lastName.eq("Cooper")).queryValue()
                        .ge(createWhenClause(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#gt(T)
     */
    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).gt("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#gt(Predicand<T>)
     */
    @Override
    public void test_gt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<String> list = employee.lastName
                .where(createWhenClause(employee)
                        .gt(cooper.firstName.where(cooper.lastName.eq("Cooper")).queryValue()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#gt(Predicand<T>)
     */
    @Override
    public void test_gt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<String> list = employee.lastName
                .where(cooper.firstName.where(cooper.lastName.eq("Cooper")).queryValue()
                        .gt(createWhenClause(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(0, list.size());
    }

    /**
     * Test AbstractSearchedWhenClause#in(InPredicateValue<T>)
     */
    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = employee.lastName.where(whenClause.in(james.firstName.where(james.firstName.like("J%"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#in(T, T[])
     */
    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).in("Margaret", (String) null))
                .orderBy(employee.lastName)
                .list(getEngine());
        // NULLs should not match to (String)null
        assertEquals(Arrays.asList("Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#in(InPredicateValue<T>)
     */
    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee overpaid = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(overpaid);
        final List<String> list = employee.lastName.where(employee.firstName.in(whenClause))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Employee employee = new Employee();
        final Employee overpaid = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(overpaid);
        final List<String> list = employee.lastName.where(employee.firstName.ge(whenClause.all()))
                .orderBy(employee.lastName)
                .list(getEngine());
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            // //ignores nulls for all()
            assertEquals(Arrays.asList("Redwood"), list);
        } else {
            // whenClause contains nulls, so >= is false for them
            assertEquals(0, list.size());
        }
    }

    @Override
    public void test_any_() throws Exception {
        final Employee employee = new Employee();
        final Employee overpaid = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(overpaid);
        final List<String> list = employee.lastName.where(employee.firstName.ne(whenClause.any()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Employee employee = new Employee();
        final Employee overpaid = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(overpaid);
        final List<String> list = employee.lastName.where(employee.firstName.eq(whenClause.some()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }



    /**
     * Test AbstractSearchedWhenClause#intersectAll(QueryPrimary<T>)
     */
    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.intersectAll(new Employee().firstName).list(getEngine());
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryTerm#intersectAll(QueryPrimary<T>)
     */
    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = new Employee().firstName.intersectAll(whenClause).list(getEngine());
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#intersectDistinct(QueryPrimary<T>)
     */
    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.intersectDistinct(new Employee().firstName).list(getEngine());
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryTerm#intersectDistinct(QueryPrimary<T>)
     */
    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = new Employee().firstName.intersectDistinct(whenClause).list(getEngine());
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#intersect(QueryPrimary<T>)
     */
    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.intersect(new Employee().firstName).list(getEngine());
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryTerm#intersect(QueryPrimary<T>)
     */
    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = new Employee().firstName.intersect(whenClause).list(getEngine());
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#isNotNull()
     */
    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = employee.lastName.where(whenClause.isNotNull())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#isNull()
     */
    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = employee.lastName.where(whenClause.isNull())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#label(Label)
     */
    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        Label label = new Label();
        final List<String> list = whenClause.label(label).orderBy(label).list(getEngine());
        final List<String> expected;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            // database(s) with NULLS FIRST default
            expected = Arrays.asList(null, null, null, "James", "Margaret");
        } else {
            expected = Arrays.asList("James", "Margaret", null, null, null);
        }
        assertEquals(expected, list);

    }

    /**
     * Test AbstractSearchedWhenClause#le(T)
     */
    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).le("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#le(Predicand<T>)
     */
    @Override
    public void test_le_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<String> list = employee.lastName
                .where(createWhenClause(employee)
                        .le(cooper.firstName.where(cooper.lastName.eq("Cooper")).queryValue()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#le(Predicand<T>)
     */
    @Override
    public void test_le_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<String> list = employee.lastName
                .where(cooper.firstName.where(cooper.lastName.eq("Cooper")).queryValue()
                        .le(createWhenClause(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#like(String)
     */
    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).like("Jam%"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#like(StringExpression<String>)
     */
    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).like(employee.firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#limit(int)
     */
    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    /**
     * Test AbstractSearchedWhenClause#limit(int, int)
     */
    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .limit(2, 10).list(getEngine());
        assertEquals(3, list.size());
    }

    /**
     * Test AbstractSearchedWhenClause#list(QueryEngine, Option[])
     */
    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("Margaret"));
        assertTrue(list.toString(), list.isEmpty());
    }

    /**
     * Test AbstractSearchedWhenClause#lt(T)
     */
    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).lt("Ken"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#lt(Predicand<T>)
     */
    @Override
    public void test_lt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final List<String> list = employee.lastName
                .where(createWhenClause(employee)
                        .lt(redwood.firstName.where(redwood.lastName.eq("Redwood")).queryValue()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#lt(Predicand<T>)
     */
    @Override
    public void test_lt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        final List<String> list = employee.lastName
                .where(cooper.firstName.where(cooper.lastName.eq("Cooper")).queryValue()
                        .lt(createWhenClause(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#map(Mapper<R>)
     */
    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<Long> list = createNumericWC(employee)
                .map(Mappers.LONG)
                .list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(3000L));
        assertTrue(list.toString(), list.remove(3000L));
        assertTrue(list.toString(), list.isEmpty());
    }

    /**
     * Test AbstractSearchedWhenClause#max()
     */
    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee).max().list(getEngine());
        assertEquals(Arrays.asList("Margaret"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#min()
     */
    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee).min().list(getEngine());
        assertEquals(Arrays.asList("James"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#mult(Factor<?>)
     */
    @Override
    public void test_mult_Factor() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = whenClause.mult(2)
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#mult(Number)
     */
    @Override
    public void test_mult_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = whenClause.mult(employee.salary.div(1500))
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  Term#mult(Factor<?>)
     */
    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = employee.salary.div(1500).mult(whenClause)
                .map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#ne(T)
     */
    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).ne("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#ne(Predicand<T>)
     */
    @Override
    public void test_ne_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final List<String> list = employee.lastName
                .where(createWhenClause(employee)
                        .ne(redwood.firstName.where(redwood.lastName.eq("Redwood")).queryValue()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#ne(Predicand<T>)
     */
    @Override
    public void test_ne_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final List<String> list = employee.lastName
                .where(redwood.firstName.where(redwood.lastName.eq("Redwood")).queryValue()
                        .ne(createWhenClause(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#notIn(InPredicateValue<T>)
     */
    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = employee.lastName.where(whenClause.notIn(james.firstName.where(james.firstName.like("J%"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#notIn(T, T[])
     */
    @Override
    public void test_notIn_Object_Object() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.where(createWhenClause(employee).notIn("nobody", "somebody"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            // for NULLs NOT IN is false
            assertEquals(Arrays.asList("First", "Redwood"), list);
        } catch (SQLException e) {
// java.lang.NullPointerException
//	at org.apache.derby.exe.acf0e9813dx0145x3c16x92e8x000005a800f831.e4(Unknown Source)
//	at org.apache.derby.impl.services.reflect.DirectCall.invoke(Unknown Source)
//	at org.apache.derby.impl.sql.execute.ProjectRestrictResultSet.getNextRowCore(Unknown Source)
//	at org.apache.derby.impl.sql.execute.ProjectRestrictResultSet.getNextRowCore(Unknown Source)
//	at org.apache.derby.impl.sql.execute.SortResultSet.getRowFromResultSet(Unknown Source)
//	at org.apache.derby.impl.sql.execute.SortResultSet.getNextRowFromRS(Unknown Source)
//	at org.apache.derby.impl.sql.execute.SortResultSet.loadSorter(Unknown Source)
//	at org.apache.derby.impl.sql.execute.SortResultSet.openCore(Unknown Source)
//	at org.apache.derby.impl.sql.execute.BasicNoPutResultSetImpl.open(Unknown Source)
//	at org.apache.derby.impl.sql.GenericPreparedStatement.executeStmt(Unknown Source)
//	at org.apache.derby.impl.sql.GenericPreparedStatement.execute(Unknown Source)
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#notIn(InPredicateValue<T>)
     */
    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Employee overpaid = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(overpaid);
        final List<String> list = employee.lastName.where(employee.firstName.notIn(whenClause))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(0, list.size());
    }

    /**
     * Test AbstractSearchedWhenClause#notLike(String)
     */
    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).notLike("Mar%"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#notLike(StringExpression<String>)
     */
    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final Employee redwood = new Employee();
        final List<String> list = employee.lastName
                .where(redwood.firstName.where(redwood.lastName.eq("Redwood")).queryValue()
                        .notLike(createWhenClause(employee)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#nullsFirst()
     */
    @Override
    public void test_nullsFirst_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClause(employee).nullsFirst(), employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS FIRST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#nullsLast()
     */
    @Override
    public void test_nullsLast_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClause(employee).nullsLast(), employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS LAST:
                // You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'NULLS LAST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#opposite()
     */
    @Override
    public void test_opposite_() throws Exception {
            final Employee employee = new Employee();
            final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
            final List<Pair<Double, String>> list = whenClause.opposite()
                    .pair(employee.lastName)
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList(
                    Pair.make((Double)null, "Cooper"),
                    Pair.make(-3000.0, "First"),
                    Pair.make((Double)null, "March"),
                    Pair.make((Double)null, "Pedersen"),
                    Pair.make(-3000.0, "Redwood")
            ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#orElse(ElseClause<T>)
     */
    @Override
    public void test_orElse_ElseClause() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClause(employee).orElse(employee.lastName)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", "Cooper"),
                Pair.make("James", "First"),
                Pair.make("March", "March"),
                Pair.make("Pedersen", "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> SearchedWhenClauseBaseList#orElse(ElseClause<T>)
     */
    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.deptId.isNull().then(employee.lastName).orElse(createWhenClause(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", "Cooper"),
                Pair.make("James", "First"),
                Pair.make((String)null, "March"),
                Pair.make((String)null, "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#orWhen(SearchedWhenClause<T>)
     */
    @Override
    public void test_orWhen_SearchedWhenClause() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClause(employee).orWhen(employee.deptId.isNull().then(employee.lastName))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", "Cooper"),
                Pair.make("James", "First"),
                Pair.make(null, "March"),
                Pair.make(null, "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> SearchedWhenClauseBaseList#orWhen(SearchedWhenClause<T>)
     */
    @Override
    public void test_orWhen_SearchedWhenClauseBaseList_SearchedWhenClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.deptId.isNull().then(employee.lastName).orWhen(createWhenClause(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", "Cooper"),
                Pair.make("James", "First"),
                Pair.make((String)null, "March"),
                Pair.make((String)null, "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#orWhen(ThenNullClause)
     */
    @Override
    public void test_orWhen_ThenNullClause() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = createWhenClause(employee)
                .orWhen(employee.deptId.isNull().then(employee.lastName))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", "Cooper"),
                Pair.make("James", "First"),
                Pair.make(null, "March"),
                Pair.make(null, "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBody#orderBy(SortSpecification, SortSpecification[])
     */
    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            // database(s) with NULLS FIRST default
            expected = Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood");
        } else {
            expected = Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen");
        }
        final List<String> list = employee.lastName.orderBy(createWhenClause(employee), employee.lastName)
                .list(getEngine());
        assertEquals(expected, list);
    }

    /**
     * Test AbstractSearchedWhenClause#orderBy(SortSpecification, SortSpecification[])
     */
    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                (String)null,
                "James",
                (String)null,
                (String)null,
                "Margaret"
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#pair(SelectList<U>)
     */
    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> list = createWhenClause(employee)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((String)null, "Cooper"),
                Pair.make("James", "First"),
                Pair.make((String)null, "March"),
                Pair.make((String)null, "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T, U> SelectList#pair(SelectList<U>)
     */
    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> list = employee.lastName
                .pair(createWhenClause(employee))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", (String)null),
                Pair.make("First", "James"),
                Pair.make("March", (String)null),
                Pair.make("Pedersen", (String)null),
                Pair.make("Redwood", "Margaret")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#param()
     */
    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        final DynamicParameter<String> param = whenClause.param();
        param.setValue("Margaret");
        final List<String> list = employee.lastName.where(whenClause.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#param(T)
     */
    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        final DynamicParameter<String> param = whenClause.param("Margaret");
        final List<String> list = employee.lastName.where(whenClause.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#positionOf(String)
     */
    @Override
    public void test_positionOf_String() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        final List<Integer> list = whenClause.positionOf("garet").where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    /**
     * Test AbstractSearchedWhenClause#positionOf(StringExpression<?>)
     */
    @Override
    public void test_positionOf_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        final List<Integer> list = whenClause.positionOf(employee.firstName).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList(1), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  StringExpression#positionOf(StringExpression<?>)
     */
    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        final List<Integer> list = employee.firstName.positionOf(whenClause).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList(1), list);
    }

    /**
     * Test AbstractSearchedWhenClause#queryValue()
     */
    @Override
    public void test_queryValue_() throws Exception {
        final MyDual myDual = new MyDual();
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                myDual.dummy.eq("X").then(myDual.dummy);
        final List<Pair<String, String>> list = employee.lastName.pair(whenClause.queryValue()).
                where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(Pair.make("Cooper", "X"), list.get(0));
    }

    /**
     * Test AbstractSearchedWhenClause#scroll(QueryEngine, Callback<T>, Option[])
     */
    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected = new ArrayList<>(Arrays.asList(null, null, null, "James", "Margaret"));
        createWhenClause(employee)
                .scroll(getEngine(), new Callback<String>() {
                    @Override
                    public boolean iterate(final String s) throws SQLException {
                        assertTrue(expected.toString(), expected.remove(s));
                        return true;
                    }
                });

        assertTrue(expected.toString(), expected.isEmpty());
    }

    /**
     * Test AbstractSearchedWhenClause#selectAll()
     */
    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .selectAll().list(getEngine());
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove(null));
        assertTrue(list.toString(), list.remove("James"));
        assertTrue(list.toString(), list.remove("Margaret"));
        assertTrue(list.toString(), list.isEmpty());
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> ColumnName#set(ValueExpression<T>)
     */
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
        final AbstractSearchedWhenClause<Integer> whenClause =
                insertTable.text.eq("abc").then(insertTable.payload);
        insertTable.update(insertTable.payload.set(whenClause)).execute(getEngine());
        final List<Pair<String, Integer>> list = insertTable.text.pair(insertTable.payload)
                .orderBy(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("abc", 1),
                Pair.make("def", (Integer)null),
                Pair.make("xyz", (Integer)null)),
                list);
    }

    /**
     * Test AbstractSearchedWhenClause#showQuery(Dialect, Option[])
     */
    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = createWhenClause(employee).showQuery(getEngine().getDialect());
        final Pattern expected;
        expected = Pattern.compile("SELECT CASE WHEN ([A-Z][A-Z0-9]*)\\.emp_id = ([A-Z][A-Z0-9]*)\\.emp_id THEN \\1\\.first_name END AS [A-Z][A-Z0-9]* FROM employee AS \\1 LEFT JOIN department AS ([A-Z][A-Z0-9]*) LEFT JOIN employee AS \\2 ON \\2\\.emp_id = \\3\\.manager_id ON \\3\\.dept_id = \\1\\.dept_id");
        assertTrue(sql, expected.matcher(sql).matches());
    }

    /**
     * Test AbstractSearchedWhenClause#sub(Number)
     */
    @Override
    public void test_sub_Number() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = whenClause.sub(100.0).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(2900.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(2900.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  NumericExpression#sub(Term<?>)
     */
    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = employee.salary.div(3).sub(whenClause).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(-2000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(-2000.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#sub(Term<?>)
     */
    @Override
    public void test_sub_Term() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = whenClause.sub(employee.salary.div(3)).map(Mappers.DOUBLE)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(2000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(2000.0, "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#substring(NumericExpression<?>)
     */
    @Override
    public void test_substring_NumericExpression() throws Exception {
        final Employee employee = new Employee();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(2)
                        .also(insertTable.text.set("abc")))).execute(getEngine());
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        final List<Pair<String, String>> list = whenClause.substring(insertTable.id.queryValue()).pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((String) null, "Cooper"),
                Pair.make("ames", "First"),
                Pair.make((String) null, "March"),
                Pair.make((String) null, "Pedersen"),
                Pair.make("argaret", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause#substring(NumericExpression<?>, NumericExpression<?>)
     */
    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        final Employee employee = new Employee();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("abc")))).execute(getEngine());
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        final List<Pair<String, String>> list = whenClause.substring(insertTable.id.queryValue(), insertTable.payload.queryValue())
                .pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((String) null, "Cooper"),
                Pair.make("ame", "First"),
                Pair.make((String) null, "March"),
                Pair.make((String) null, "Pedersen"),
                Pair.make("arg", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  StringExpression#substring(NumericExpression<?>)
     */
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
        final AbstractSearchedWhenClause<Integer> whenClause =
                insertTable.text.eq("abc").then(insertTable.payload);
        final List<String> list = insertTable.text.substring(whenClause)
                .orderBy(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList("c", null, null), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  StringExpression#substring(NumericExpression<?>, NumericExpression<?>)
     */
    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("abc012")))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("def345")))).execute(getEngine());
        insertTable.insert(insertTable.id.set(3)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("pqrstuv")))).execute(getEngine());
        final AbstractSearchedWhenClause<Integer> whenClause =
                insertTable.text.eq("abc012").then(insertTable.payload);
        final List<String> list = insertTable.text.substring(whenClause, insertTable.payload)
                .orderBy(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList("c01", null, null), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 2 of  StringExpression#substring(NumericExpression<?>, NumericExpression<?>)
     */
    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("abc012")))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("def345")))).execute(getEngine());
        insertTable.insert(insertTable.id.set(3)
                .also(insertTable.payload.set(3)
                        .also(insertTable.text.set("pqrstuv")))).execute(getEngine());
        final AbstractSearchedWhenClause<Integer> whenClause =
                insertTable.text.eq("abc012").then(insertTable.payload);
        final List<String> list = insertTable.text.substring(insertTable.payload, whenClause)
                .orderBy(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList("c01", null, null), list);
    }

    /**
     * Test AbstractSearchedWhenClause#substring(int)
     */
    @Override
    public void test_substring_int() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        final List<String> list = whenClause.substring(4).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("garet"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#substring(int, int)
     */
    @Override
    public void test_substring_int_int() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause(employee);
        final List<String> list = whenClause.substring(4, 3).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("gar"), list);
    }

    /**
     * Test AbstractSearchedWhenClause#sum()
     */
    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Number> list = whenClause.sum().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(6000.0, list.get(0).doubleValue());
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> BooleanExpression#then(ValueExpression<T>)
     */
    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.eq("James").then(createWhenClause(employee)).orElse(employee.lastName)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make(null, "Cooper"),
                Pair.make("James", "First"),
                Pair.make("March", "March"),
                Pair.make("Pedersen", "Pedersen"),
                Pair.make("Redwood", "Redwood")
        ), list);
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBodyScalar#unionAll(QueryTerm<T>)
     */
    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = new Employee().firstName.unionAll(whenClause).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#unionAll(QueryTerm<T>)
     */
    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.unionAll(new Employee().firstName).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBodyScalar#unionDistinct(QueryTerm<T>)
     */
    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = new Employee().firstName.unionDistinct(whenClause).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#unionDistinct(QueryTerm<T>)
     */
    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.unionDistinct(new Employee().firstName).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }
    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBodyScalar#union(QueryTerm<T>)
     */
    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = new Employee().firstName.union(whenClause).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#union(QueryTerm<T>)
     */
    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.union(new Employee().firstName).list(getEngine());
            assertTrue(list.toString(), list.remove(null));
            assertTrue(list.toString(), list.remove("Bill"));
            assertTrue(list.toString(), list.remove("Alex"));
            assertTrue(list.toString(), list.remove("James"));
            assertTrue(list.toString(), list.remove("Margaret"));
            assertTrue(list.toString(), list.isEmpty());
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    /**
     * Test AbstractSearchedWhenClause#where(WhereClause)
     */
    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .where(employee.firstName.eq("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                (String) null,
                "James"
        ), list);
    }

}
