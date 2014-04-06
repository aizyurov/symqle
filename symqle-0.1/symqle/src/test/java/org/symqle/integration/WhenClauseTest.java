package org.symqle.integration;

import junit.framework.AssertionFailedError;
import net.sf.cglib.core.TypeUtils;
import org.symqle.common.CoreMappers;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.InsertTable;
import org.symqle.sql.Mappers;
import org.symqle.sql.Params;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractSearchedWhenClause;
import org.symqle.testset.AbstractSearchedWhenClauseTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
     * Test AbstractSearchedWhenClause#asPredicate()
     */
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
            final AbstractSearchedWhenClause<Integer> whenClauseBaseList =
                    insertTable.text.eq("abc").then(insertTable.payload);;
            final List<String> list = insertTable.text.where(whenClauseBaseList.asPredicate()).list(getEngine());
            // "abc" -> 1 -> true
            // "def" -> null -> false
            // "xyz" -> null -> false
            assertEquals(Arrays.asList("abc"), list);
        } catch (SQLException e) {
            // ERROR 42846: Cannot convert types 'INTEGER' to 'BOOLEAN'
            // org.postgresql.util.PSQLException: ERROR: cannot cast type numeric to boolean
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    /**
     * Test AbstractSearchedWhenClause#asc()
     */
    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected;
        if ("MySQL".equals(getDatabaseName())) {
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
        if ("MySQL".equals(getDatabaseName())) {
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
            expectSQLException(e, "Apache Derby");
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
        if ("MySQL".equals(getDatabaseName())) {
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
    public void test_eq_Predicand() throws Exception {
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
    public void test_eq_Predicand_Predicand_1() throws Exception {
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
            expectSQLException(e, "MySQL");
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
            expectSQLException(e, "MySQL");
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
            expectSQLException(e, "MySQL");
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
            expectSQLException(e, "MySQL");
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
            expectSQLException(e, "MySQL");
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
            expectSQLException(e, "MySQL");
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
            expectSQLException(e, "Apache Derby", "PostgreSQL");
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
    public void test_ge_Predicand() throws Exception {
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
    public void test_ge_Predicand_Predicand_1() throws Exception {
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
    public void test_gt_Predicand() throws Exception {
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
    public void test_gt_Predicand_Predicand_1() throws Exception {
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

    /**
     * Test AbstractSearchedWhenClause#intersectAll(QueryPrimary<T>)
     */
    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryTerm#intersectAll(QueryPrimary<T>)
     */
    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#intersectDistinct(QueryPrimary<T>)
     */
    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryTerm#intersectDistinct(QueryPrimary<T>)
     */
    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#intersect(QueryPrimary<T>)
     */
    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryTerm#intersect(QueryPrimary<T>)
     */
    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#isNotNull()
     */
    @Override
    public void test_isNotNull_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#isNull()
     */
    @Override
    public void test_isNull_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#label(Label)
     */
    @Override
    public void test_label_Label() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#le(T)
     */
    @Override
    public void test_le_Object() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#le(Predicand<T>)
     */
    @Override
    public void test_le_Predicand() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#le(Predicand<T>)
     */
    @Override
    public void test_le_Predicand_Predicand_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#like(String)
     */
    @Override
    public void test_like_String() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#like(StringExpression<String>)
     */
    @Override
    public void test_like_StringExpression() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#limit(int)
     */
    @Override
    public void test_limit_int() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#limit(int, int)
     */
    @Override
    public void test_limit_int_int() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#list(QueryEngine, Option[])
     */
    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#lt(T)
     */
    @Override
    public void test_lt_Object() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#lt(Predicand<T>)
     */
    @Override
    public void test_lt_Predicand() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#lt(Predicand<T>)
     */
    @Override
    public void test_lt_Predicand_Predicand_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#map(Mapper<R>)
     */
    @Override
    public void test_map_Mapper() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#max()
     */
    @Override
    public void test_max_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#min()
     */
    @Override
    public void test_min_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#mult(Factor<?>)
     */
    @Override
    public void test_mult_Factor() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#mult(Number)
     */
    @Override
    public void test_mult_Number() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  Term#mult(Factor<?>)
     */
    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#ne(T)
     */
    @Override
    public void test_ne_Object() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#ne(Predicand<T>)
     */
    @Override
    public void test_ne_Predicand() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#ne(Predicand<T>)
     */
    @Override
    public void test_ne_Predicand_Predicand_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#notIn(InPredicateValue<T>)
     */
    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#notIn(T, T[])
     */
    @Override
    public void test_notIn_Object_Object() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> Predicand#notIn(InPredicateValue<T>)
     */
    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#notLike(String)
     */
    @Override
    public void test_notLike_String() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#notLike(StringExpression<String>)
     */
    @Override
    public void test_notLike_StringExpression() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#nullsFirst()
     */
    @Override
    public void test_nullsFirst_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#nullsLast()
     */
    @Override
    public void test_nullsLast_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#opposite()
     */
    @Override
    public void test_opposite_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#orElse(ElseClause<T>)
     */
    @Override
    public void test_orElse_ElseClause() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> SearchedWhenClauseBaseList#orElse(ElseClause<T>)
     */
    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#orWhen(SearchedWhenClause<T>)
     */
    @Override
    public void test_orWhen_SearchedWhenClause() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> SearchedWhenClauseBaseList#orWhen(SearchedWhenClause<T>)
     */
    @Override
    public void test_orWhen_SearchedWhenClauseBaseList_SearchedWhenClause_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#orWhen(ThenNullClause)
     */
    @Override
    public void test_orWhen_ThenNullClause() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBody#orderBy(SortSpecification, SortSpecification[])
     */
    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#orderBy(SortSpecification, SortSpecification[])
     */
    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#pair(SelectList<U>)
     */
    @Override
    public void test_pair_SelectList() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T, U> SelectList#pair(SelectList<U>)
     */
    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#param()
     */
    @Override
    public void test_param_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#param(T)
     */
    @Override
    public void test_param_Object() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#positionOf(String)
     */
    @Override
    public void test_positionOf_String() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#positionOf(StringExpression<?>)
     */
    @Override
    public void test_positionOf_StringExpression() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  StringExpression#positionOf(StringExpression<?>)
     */
    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#queryValue()
     */
    @Override
    public void test_queryValue_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#scroll(QueryEngine, Callback<T>, Option[])
     */
    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#selectAll()
     */
    @Override
    public void test_selectAll_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> ColumnName#set(ValueExpression<T>)
     */
    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#showQuery(Dialect, Option[])
     */
    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#sub(Number)
     */
    @Override
    public void test_sub_Number() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  NumericExpression#sub(Term<?>)
     */
    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#sub(Term<?>)
     */
    @Override
    public void test_sub_Term() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#substring(NumericExpression<?>)
     */
    @Override
    public void test_substring_NumericExpression() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#substring(NumericExpression<?>, NumericExpression<?>)
     */
    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  StringExpression#substring(NumericExpression<?>)
     */
    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of  StringExpression#substring(NumericExpression<?>, NumericExpression<?>)
     */
    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 2 of  StringExpression#substring(NumericExpression<?>, NumericExpression<?>)
     */
    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#substring(int)
     */
    @Override
    public void test_substring_int() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#substring(int, int)
     */
    @Override
    public void test_substring_int_int() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#sum()
     */
    @Override
    public void test_sum_() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> BooleanExpression#then(ValueExpression<T>)
     */
    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBodyScalar#unionAll(QueryTerm<T>)
     */
    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#unionAll(QueryTerm<T>)
     */
    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBodyScalar#unionDistinct(QueryTerm<T>)
     */
    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#unionDistinct(QueryTerm<T>)
     */
    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause as argument 1 of <T> QueryExpressionBodyScalar#union(QueryTerm<T>)
     */
    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#union(QueryTerm<T>)
     */
    @Override
    public void test_union_QueryTerm() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Test AbstractSearchedWhenClause#where(WhereClause)
     */
    @Override
    public void test_where_WhereClause() throws Exception {
        throw new RuntimeException("Not implemented");
    }

    public void testPair() throws Exception {
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

    public void testOrderBy() throws Exception {
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

    public void testWhere() throws Exception {
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

    public void testMap() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee).map(CoreMappers.STRING)
                .list(getEngine());

        assertEquals(Arrays.asList("(null)", "(null)", "(null)", "James", "Margaret"), replaceNullsAndSort(list));
    }

    public void testList() throws Exception {
    }

    public void testCast() throws Exception {
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .selectAll()
                .list(getEngine());

        assertEquals(Arrays.asList("(null)", "(null)", "(null)", "James", "Margaret"), replaceNullsAndSort(list));
    }

    public void testDistinct() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee)
                .distinct()
                .list(getEngine());
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
            final List<String> list = whenClause.forUpdate().list(getEngine());
            assertEquals(Arrays.asList("(null)", "(null)", "(null)", "James", "Margaret"), replaceNullsAndSort(list));
        } catch (SQLException e) {
            //derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            //org.postgresql.util.PSQLException: ERROR: SELECT FOR UPDATE/SHARE cannot be applied to the nullable side of an outer join
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }

    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
            final List<String> list = whenClause.forReadOnly().list(getEngine());
            assertEquals(Arrays.asList("(null)", "(null)", "(null)", "James", "Margaret"), replaceNullsAndSort(list));
    }


    public void testElseParam() throws Exception {
    final Employee employee = new Employee();
        final List<Pair<String,String>> list = createWhenClause(employee).orElse(Params.p("Anonymous"))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
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
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("James+", "Cooper"),
                Pair.make("James", "First"),
                Pair.make("Bill+", "March"),
                Pair.make("Alex+", "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    public void testEq() throws Exception {
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).ne("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).gt("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    public void testGe() throws Exception {
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).lt("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(createWhenClause(employee).le("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First"), list);
    }

    public void testInArgument() throws Exception {
        final Employee pattern = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(pattern);
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.in(whenClause))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testExists() throws Exception {
    }

    public void testContains() throws Exception {
        final Employee pattern = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(pattern);
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(whenClause.contains("Margaret"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);

        final List<String> emptyList = employee.lastName.where(whenClause.contains("Bill"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(0, emptyList.size());

    }

    public void testIn() throws Exception {
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee james = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = employee.lastName.where(whenClause.notIn(james.firstName.where(james.firstName.like("J%"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }
    public void testInList() throws Exception {
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(createWhenClause(employee).notIn("nobody", "somebody"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
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
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testAsElseArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.eq("Bill").then(employee.firstName)
                .orElse(createWhenClause(employee))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
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
                .list(getEngine());
        // order is unspecified;
        // assume NULLS LAST by default
        // first sort field is "James", "Margaret", NULL, NULL, NULL,
        try {
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (AssertionFailedError e) {
            // mysql: default is NULLS FIRST
            if ("MySQL".equals(getDatabaseName())) {
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
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS FIRST
            expectSQLException(e, "MySQL");
        }
    }

    public void testNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(createWhenClause(employee).nullsLast(), employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "Redwood", "Cooper", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // mysql: does not support NULLS LAST:
                // You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'NULLS LAST
            expectSQLException(e, "MySQL");
        }
    }

    public void testAsc() throws Exception {
    }

    public void testDesc() throws Exception {
    }

    // TODO: derby objects if the argument of second THEN is ?
    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Double, String>> list = whenClause.opposite().pair(employee.lastName)
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

    public void testAdd() throws Exception {
    }

    public void testSub() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Pair<Number, String>> list = whenClause.sub(100.0).pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
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
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make((Double)null, "Cooper"),
                Pair.make(6000.0, "First"),
                Pair.make((Double)null, "March"),
                Pair.make((Double)null, "Pedersen"),
                Pair.make(6000.0, "Redwood")
        ), convertToDoubleStringPairList(list));
    }

    public void testDiv() throws Exception {
    }

    private List<Pair<Double, String>> convertToDoubleStringPairList(List<Pair<Number, String>> source) {
        final List<Pair<Double, String>> result = new ArrayList<Pair<Double, String>>();
        for (Pair<Number, String> p : source) {
            result.add(Pair.make(p.first() == null ? null : p.first().doubleValue(), p.second()));
        }
        return result;
    }

    public void testConcat() throws Exception {
    }

    public void testCollate() throws Exception {
    }


    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = whenClause.unionAll(new Employee().firstName).list(getEngine());
        assertEquals(Arrays.asList(
                "(null)", "(null)", "(null)", "Alex", "Bill", "James", "James", "James", "Margaret", "Margaret"
        ), replaceNullsAndSort(list));
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = whenClause.unionDistinct(new Employee().firstName).list(getEngine());
        assertEquals(Arrays.asList(
                "(null)", "Alex", "Bill", "James", "Margaret"
        ), replaceNullsAndSort(list));
    }


    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        final List<String> list = whenClause.union(new Employee().firstName).list(getEngine());
        assertEquals(Arrays.asList(
                "(null)", "Alex", "Bill", "James", "Margaret"
        ), replaceNullsAndSort(list));
    }

    public void testExceptAll() throws Exception {
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.exceptDistinct(new Employee().firstName).list(getEngine());
            assertEquals(Arrays.asList(
                    "(null)"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.except(new Employee().firstName).list(getEngine());
            assertEquals(Arrays.asList(
                    "(null)"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.intersectAll(new Employee().firstName).list(getEngine());
            assertEquals(Arrays.asList(
                    "James", "Margaret"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.intersectDistinct(new Employee().firstName).list(getEngine());
            assertEquals(Arrays.asList(
                    "James", "Margaret"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<String> whenClause =
                createWhenClause(employee);
        try {
            final List<String> list = whenClause.intersect(new Employee().firstName).list(getEngine());
            assertEquals(Arrays.asList(
                    "James", "Margaret"
            ), replaceNullsAndSort(list));
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    public void testCount() throws Exception {
    }

    public void testCountDistinct() throws Exception {
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee).min().list(getEngine());
        assertEquals(Arrays.asList("James"), list);
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createWhenClause(employee).max().list(getEngine());
        assertEquals(Arrays.asList("Margaret"), list);
    }

    public void testSum() throws Exception {
        final Employee employee = new Employee();
        final AbstractSearchedWhenClause<Double> whenClause = createNumericWC(employee);
        final List<Number> list = whenClause.sum().list(getEngine());
        assertEquals(1, list.size());
        assertEquals(6000.0, list.get(0).doubleValue());

    }

    public void testAvg() throws Exception {
    }
}
