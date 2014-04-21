package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.CoreMappers;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.integration.model.One;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractQueryExpressionBasic;
import org.symqle.sql.AbstractValueExpressionPrimary;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Label;
import org.symqle.sql.Mappers;
import org.symqle.testset.AbstractValueExpressionPrimaryTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class ValueExpressionPrimaryTest extends AbstractIntegrationTestBase implements AbstractValueExpressionPrimaryTestSet {

    /**
     * Returns the alphabetically first last name of employees of this department
     * @param department
     * @return
     */
    private AbstractValueExpressionPrimary<String> createPrimary(final Department department) {
        final Employee employee = new Employee();
        return employee.lastName.min().where(employee.deptId.eq(department.deptId)).queryValue();
    }

    @Override
    public void test_add_Number() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                primary.add(500.0).map(Mappers.DOUBLE)
                        .pair(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2500.0, "DEV"), Pair.make(2500.0, "HR")), list);
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                department.manager().salary.add(primary).map(Mappers.DOUBLE).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(5000.0, "DEV"), Pair.make(5000.0, "HR")), list);

    }

    @Override
    public void test_add_Term() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                primary.add(department.manager().salary).map(Mappers.DOUBLE).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(5000.0, "DEV"), Pair.make(5000.0, "HR")), list);
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
            final List<String> list = employee.lastName
                    .where(employee.salary.in(employee.department().manager().salary.asInValueList().append(primary)))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            //  org.postgresql.util.PSQLException: ERROR: aggregates not allowed in WHERE clause
            expectSQLException(e, SupportedDb.POSTGRESQL);
        }
    }

    @Override
    public void test_asInValueList_() throws Exception {
        try {
            final Department department = new Department();
            final Employee employee = new Employee();
            final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
            final AbstractQueryExpressionBasic<String> query = employee.lastName
                    .where(employee.salary.in(primary.asInValueList().append(employee.department().manager().salary)))
                    .orderBy(employee.lastName);
            System.out.println(query.showQuery(getEngine().getDialect()));
            final List<String> list = query
                    .list(getEngine());
            assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // ERROR 42X01: Syntax error: Encountered "," at line 1, column 350.
            //  org.postgresql.util.PSQLException: ERROR: aggregates not allowed in WHERE clause
            //  org.h2.jdbc.JdbcSQLException: Syntax error in SQL statement
            expectSQLException(e, SupportedDb.POSTGRESQL, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_asBoolean_() throws Exception {
        final Employee employee = new Employee();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        final String trueText = SupportedDb.MYSQL.equals(getDatabaseName()) ? "1" : "true";
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set(trueText))).execute(getEngine());
        final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

        final List<Integer> list = employee.empId.count().where(primary.asBoolean()).list(getEngine());
        assertEquals(Arrays.asList(5), list);

        insertTable.update(insertTable.text.set("false")).execute(getEngine());
        final List<Integer> list2 = employee.empId.count().where(primary.asBoolean()).list(getEngine());
        assertEquals(Arrays.asList(0), list2);
    }

    @Override
    public void test_asc_() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .orderBy(createPrimary(department).asc())
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_avg_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        // where is added to pull department to outer scope
        final List<Number> list = employee.salary.max().where(employee.deptId.eq(department.deptId)).queryValue().avg().where(department.deptId.isNotNull()).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(3000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_cast_String() throws Exception {
        final Department department = new Department();
        final List<Pair<String, String>> list = createPrimary(department).cast("CHAR(6)").pair(department.deptName).orderBy(department.deptName)
                .list(getEngine());
        final List<Pair<String, String>> expected;
        if (NO_PADDING_ON_CAST_TO_CHAR.contains(getDatabaseName())) {
            expected = Arrays.asList(Pair.make("First", "DEV"), Pair.make("March", "HR"));
        } else {
            expected = Arrays.asList(Pair.make("First ", "DEV"), Pair.make("March ", "HR"));
        }
        assertEquals(expected, list);

    }

    @Override
    public void test_charLength_() throws Exception {
        final Department department = new Department();
        final List<Integer> list = createPrimary(department).charLength()
                .where(department.deptName.eq("DEV")).list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_collate_String() throws Exception {
        final Department department = new Department();
        final Label label = new Label();
        try {
            final List<String> list = createPrimary(department)
                    .collate(validCollationNameForVarchar())
                    .concat("-").concat(department.deptName)
                    .label(label)
                    .orderBy(label)
                    .list(getEngine());
            assertEquals(Arrays.asList("First-DEV", "March-HR"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE"
            // org.h2.jdbc.JdbcSQLException: Syntax error in SQL statement
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final MyDual myDual = new MyDual();
        final AbstractValueExpressionPrimary<String> primary = myDual.dummy.queryValue();
        final List<String> list = primary.compileQuery(getEngine(), Option.allowNoTables(true)).list();
        assertEquals(Arrays.asList("X"), list);
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Department department = new Department();
        final List<String> list = createPrimary(department).concat(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("FirstDEV", "MarchHR"), list);
    }

    @Override
    public void test_concat_String() throws Exception {
        final Department department = new Department();
        final List<String> list = createPrimary(department).concat("-").concat(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("First-DEV", "March-HR"), list);
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName.concat("-").concat(createPrimary(department))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV-First", "HR-March"), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName.where(createPrimary(department).contains("March"))
                .orderBy(department.deptName)
                .list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Department department = new Department();
        final List<Integer> list = createPrimary(department).countDistinct()
                .where(department.deptId.isNotNull())
                .list(getEngine());
        assertEquals(Arrays.asList(2), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Department department = new Department();
        final List<Integer> list = createPrimary(department).count()
                .where(department.deptId.isNotNull())
                .list(getEngine());
        assertEquals(Arrays.asList(2), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .orderBy(createPrimary(department).desc())
                .list(getEngine());
        assertEquals(Arrays.asList("HR", "DEV"), list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final MyDual myDual = new MyDual();
        final AbstractValueExpressionPrimary<String> primary = myDual.dummy.queryValue();
        final List<String> list = primary.distinct().list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList("X"), list);
    }

    @Override
    public void test_div_Factor() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.max().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                primary.div(department.manager().salary)
                        .map(Mappers.DOUBLE)
                        .pair(department.deptName)
                .where(department.deptName.eq("DEV"))
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1.0, "DEV")), list);
    }

    @Override
    public void test_div_Number() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                primary.div(2).map(Mappers.DOUBLE).pair(department.deptName)
                .where(department.deptName.eq("DEV"))
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1000.0, "DEV")), list);
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.max().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                department.manager().salary.div(primary)
                        .map(Mappers.DOUBLE)
                        .pair(department.deptName)
                .where(department.deptName.eq("DEV"))
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1.0, "DEV")), list);
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).eq("March"))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_eq_Predicand2() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).eq(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand2_1() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.eq(createPrimary(department)))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = department.manager().lastName
                    .exceptAll(primary)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("Jones"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = primary
                    .exceptAll(department.manager().lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("Jones"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = department.manager().lastName
                    .exceptDistinct(primary)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("Jones"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = primary
                    .exceptDistinct(department.manager().lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("Jones"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = department.manager().lastName
                    .except(primary)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("Jones"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = primary
                    .except(department.manager().lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("Jones"), list);
        } catch (SQLException e) {
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName.where(createPrimary(department).exists())
                .orderBy(department.deptName)
                .list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final One one = new One();
        final List<Integer> list = one.id.queryValue().forReadOnly().list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        try {
            final One one = new One();
            final List<Integer> list = one.id.queryValue().forUpdate().list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement.
            // org.h2.jdbc.JdbcSQLException: General error: "java.lang.NullPointerException"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).ge("March"))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_ge_Predicand2() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).ge(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_ge_Predicand_Predicand2_1() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.ge(createPrimary(department)))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).gt("First"))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_gt_Predicand2() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).gt(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(), list);
    }

    @Override
    public void test_gt_Predicand_Predicand2_1() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.gt(createPrimary(department)))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).in(new Department().manager().lastName))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).in("Cooper", "March"))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.in(createPrimary(department)))
                .orderBy(department.deptName)
                .list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_all_() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.eq(createPrimary(department).all()))
                .orderBy(department.deptName)
                .list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_any_() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.ne(createPrimary(department).any()))
                .orderBy(department.deptName)
                .list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_some_() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.eq(createPrimary(department).some()))
                .orderBy(department.deptName)
                .list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = primary
                    .intersectAll(department.manager().lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = department.manager().lastName
                    .intersectAll(primary)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = primary
                    .intersectDistinct(department.manager().lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = department.manager().lastName
                    .intersectDistinct(primary)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT/ALL
            expectSQLException(e, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = primary
                    .intersect(department.manager().lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        try {
            final Department department = new Department();
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
            final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

            final List<String> list = department.manager().lastName
                    .intersect(primary)
                    .list(getEngine(), Option.allowNoTables(true));
            Collections.sort(list);
            assertEquals(Arrays.asList("First"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).isNotNull())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).isNull())
                .list(getEngine());
        assertEquals(0, list.size());
    }

    @Override
    public void test_label_Label() throws Exception {
        final Department department = new Department();
        final AbstractValueExpressionPrimary<String> primary = createPrimary(department);
        final Label l = new Label();
        final List<Pair<String, String>> list = primary.label(l).pair(department.deptName)
                .orderBy(l).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("First", "DEV"), Pair.make("March", "HR")), list);
    }

    @Override
    public void test_le_Object() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).le("March"))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_le_Predicand2() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).le(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_le_Predicand_Predicand2_1() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.le(createPrimary(department)))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_like_String() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).like("_ar%"))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).like(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final One one = new One();
        final List<Integer> list = one.id.queryValue().limit(2).list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final One one = new One();
        final List<Integer> list = one.id.queryValue().limit(0, 5).list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final One one = new One();
        final List<Integer> list = one.id.queryValue().list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_countRows_() throws Exception {
        final One one = new One();
        final List<Integer> list = one.id.queryValue().countRows().list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).lt("March"))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_lt_Predicand2() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).lt(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand2_1() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.lt(createPrimary(department)))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(0, list.size());
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final One one = new One();
        final AbstractValueExpressionPrimary<String> primary = one.id.map(CoreMappers.STRING).queryValue();
        final List<String> list = primary.list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList("1"), list);
    }

    @Override
    public void test_max_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.max().where(employee.deptId.eq(department.deptId))
                .queryValue();
        final List<Double> list = primary.max().where(department.deptId.isNotNull()).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(3000.0, list.get(0));
    }

    @Override
    public void test_min_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.max().where(employee.deptId.eq(department.deptId))
                .queryValue();
        final List<Double> list = primary.min().where(department.deptId.isNotNull()).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(3000.0, list.get(0));
    }

    @Override
    public void test_mult_Factor() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Double> list = primary.mult(department.manager().salary)
                .map(Mappers.DOUBLE)
                .where(department.deptName.eq("DEV"))
                .list(getEngine());
        assertEquals(Arrays.asList(6000000.0), list);
    }

    @Override
    public void test_mult_Number() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Double> list = primary.mult(2).map(Mappers.DOUBLE)
                .where(department.deptName.eq("DEV"))
                .list(getEngine());
        assertEquals(Arrays.asList(4000.0), list);
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Double> list = department.manager().salary.mult(primary)
                .map(Mappers.DOUBLE)
                .where(department.deptName.eq("DEV"))
                .list(getEngine());
        assertEquals(Arrays.asList(6000000.0), list);
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).ne("March"))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_ne_Predicand2() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).ne(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand2_1() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.ne(createPrimary(department)))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).notIn(new Department().manager().lastName))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).notIn("Cooper", "March"))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(department.manager().lastName.notIn(createPrimary(department)))
                .orderBy(department.deptName)
                .list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).notLike("_ar%"))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV"), list);
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .where(createPrimary(department).notLike(department.manager().lastName))
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        try {
            final Department department = new Department();
            final List<String> list = department.deptName
                    .orderBy(createPrimary(department).nullsFirst())
                    .list(getEngine());
            assertEquals(Arrays.asList("DEV", "HR"), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        try {
            final Department department = new Department();
            final List<String> list = department.deptName
                    .orderBy(createPrimary(department).nullsLast())
                    .list(getEngine());
            assertEquals(Arrays.asList("DEV", "HR"), list);
        } catch (SQLException e) {
            // mysql does not support NULLS LAST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.max().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                primary.opposite().pair(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(-3000.0, "DEV"), Pair.make(-3000.0, "HR")), list);
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Department department = new Department();
        final List<Pair<String, String>> list = department.deptName.eq("HR").then(department.manager().firstName)
                .orElse(createPrimary(department))
                .pair(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make("First", "DEV"), Pair.make("Margaret", "HR")), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Department department = new Department();
        final List<String> list = department.deptName
                .orderBy(createPrimary(department))
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Department department = new Department();
        final List<String> list = createPrimary(department)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March"), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Department department = new Department();
        final List<Pair<String, String>> list = createPrimary(department)
                .pair(department.deptName)
                .orderBy(department.deptName.desc())
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make("March", "HR"), Pair.make("First", "DEV")), list);
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Department department = new Department();
        final List<Pair<String, String>> list = department.deptName
                .pair(createPrimary(department))
                .orderBy(department.deptName.desc())
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make("HR", "March"), Pair.make("DEV", "First")), list);
    }

    @Override
    public void test_param_() throws Exception {
        final Department department = new Department();
        final AbstractValueExpressionPrimary<String> primary = createPrimary(department);
        final DynamicParameter<String> param = primary.param();
        param.setValue("March");
        final List<String> list = department.deptName.where(primary.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_param_Object() throws Exception {
        final Department department = new Department();
        final AbstractValueExpressionPrimary<String> primary = createPrimary(department);
        final DynamicParameter<String> param = primary.param("March");
        final List<String> list = department.deptName.where(primary.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("HR"), list);
    }

    @Override
    public void test_positionOf_String() throws Exception {
        final Department department = new Department();
        final AbstractValueExpressionPrimary<String> primary = createPrimary(department);
        final List<Integer> list = primary.positionOf("ch").where(department.deptName.eq("HR")).list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        try {
            final Department department = new Department();
            final AbstractValueExpressionPrimary<String> primary = createPrimary(department);
            final List<Integer> list = primary.positionOf(department.manager().lastName.substring(4)).where(department.deptName.eq("DEV")).list(getEngine());
            assertEquals(Arrays.asList(4), list);
        } catch (SQLException e) {
            // Caused by: java.lang.NoSuchMethodError: org.apache.derby.iapi.types.ConcatableDataValue.locate(Lorg/apache/derby/iapi/types/StringDataValue;Lorg/apache/derby/iapi/types/NumberDataValue;Lorg/apache/derby/iapi/types/NumberDataValue;)Lorg/apache/derby/iapi/types/
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("wood"))).execute(getEngine());

        final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.positionOf(primary).where(employee.firstName.eq("Margaret"))
                .list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final One one = new One();
        final List<Integer> list = one.id.queryValue().queryValue().list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final One one = new One();
        final List<Integer> expected = new ArrayList<>(Arrays.asList(1));
        one.id.queryValue().scroll(getEngine(), new Callback<Integer>() {
            @Override
            public boolean iterate(final Integer integer) throws SQLException {
                assertTrue(expected.toString(), expected.remove(integer));
                return true;
            }
        }, Option.allowNoTables(true));
        assertTrue(expected.toString(), expected.isEmpty());
    }

    @Override
    public void test_selectAll_() throws Exception {
        final One one = new One();
        final List<Integer> list = one.id.queryValue().list(getEngine(), Option.allowNoTables(true));
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.payload.set(3))).execute(getEngine());
        assertEquals(Arrays.asList(3), insertTable.payload.list(getEngine()));

        final One one = new One();
        final AbstractValueExpressionPrimary<Integer> primary = one.id.queryValue();

        insertTable.update(insertTable.payload.set(primary)).execute(getEngine());
        assertEquals(Arrays.asList(1), insertTable.payload.list(getEngine()));
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final One one = new One();
        final String sql = one.id.queryValue().showQuery(getEngine().getDialect(), Option.allowNoTables(true));
        final String expected;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            expected = "SELECT(SELECT T0.id FROM one AS T0) AS C0";
        } else {
            expected = "SELECT(SELECT T0.id FROM one AS T0) AS C0 FROM(VALUES(1)) AS T1";
        }
        assertSimilar(expected, sql);
    }

    @Override
    public void test_sub_Number() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                primary.sub(-500.0).map(Mappers.DOUBLE)
                        .pair(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2500.0, "DEV"), Pair.make(2500.0, "HR")), list);
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                department.manager().salary.sub(primary).map(Mappers.DOUBLE).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1000.0, "DEV"), Pair.make(1000.0, "HR")), list);
    }

    @Override
    public void test_sub_Term() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.min().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Pair<Double, String>> list =
                primary.sub(department.manager().salary.opposite()).map(Mappers.DOUBLE).pair(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(5000.0, "DEV"), Pair.make(5000.0, "HR")), list);
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        final Department department = new Department();
        final AbstractValueExpressionPrimary<String> primary = createPrimary(department);
        final List<String> list = primary.substring(department.deptName.charLength())
                .where(department.deptName.eq("HR"))
                .list(getEngine());
        assertEquals(Arrays.asList("arch"), list);
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        final Department department = new Department();
        final AbstractValueExpressionPrimary<String> primary = createPrimary(department);
        final List<String> list = primary.substring(department.deptName.charLength(), department.deptName.charLength())
                .where(department.deptName.eq("HR"))
                .list(getEngine());
        assertEquals(Arrays.asList("ar"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.payload.set(3))).execute(getEngine());

        final AbstractValueExpressionPrimary<Integer> primary = insertTable.payload.queryValue();

        final Employee employee = new Employee();
        final List<String> list = employee.lastName.substring(primary).where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(Arrays.asList("dwood"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.payload.set(3))).execute(getEngine());

        final AbstractValueExpressionPrimary<Integer> primary = insertTable.payload.queryValue();

        final Employee employee = new Employee();
        final List<String> list = employee.lastName.substring(primary, primary).where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(Arrays.asList("dwo"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.payload.set(3))).execute(getEngine());

        final AbstractValueExpressionPrimary<Integer> primary = insertTable.payload.queryValue();

        final Employee employee = new Employee();
        final List<String> list = employee.lastName.substring(primary, primary).where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(Arrays.asList("dwo"), list);
    }

    @Override
    public void test_substring_int() throws Exception {
        final Department department = new Department();
        final AbstractValueExpressionPrimary<String> primary = createPrimary(department);
        final List<String> list = primary.substring(4)
                .where(department.deptName.eq("HR"))
                .list(getEngine());
        assertEquals(Arrays.asList("ch"), list);
    }

    @Override
    public void test_substring_int_int() throws Exception {
        final Department department = new Department();
        final AbstractValueExpressionPrimary<String> primary = createPrimary(department);
        final List<String> list = primary.substring(2, 2)
                .where(department.deptName.eq("HR"))
                .list(getEngine());
        assertEquals(Arrays.asList("ar"), list);
    }

    @Override
    public void test_sum_() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        // where is added to pull department to outer scope
        final AbstractValueExpressionPrimary<Double> primary = employee.salary.max().where(employee.deptId.eq(department.deptId)).queryValue();
        final List<Number> list = primary.sum().where(department.deptId.isNotNull()).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(6000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Department department = new Department();
        final List<Pair<String, String>> list = department.deptName.ne("HR").then(createPrimary(department))
                .orElse(department.manager().firstName)
                .pair(department.deptName)
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make("First", "DEV"), Pair.make("Margaret", "HR")), list);
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
        final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

        final List<String> list = department.manager().lastName
                .unionAll(primary)
                .list(getEngine(), Option.allowNoTables(true));
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "First", "Redwood"), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Department department = new Department();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
        final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

        final List<String> list = primary
                .unionAll(department.manager().lastName)
                .list(getEngine(), Option.allowNoTables(true));
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "First", "Redwood"), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
        final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

        final List<String> list = department.manager().lastName
                .unionDistinct(primary)
                .list(getEngine(), Option.allowNoTables(true));
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Department department = new Department();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
        final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

        final List<String> list = primary
                .unionDistinct(department.manager().lastName)
                .list(getEngine(), Option.allowNoTables(true));
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Department department = new Department();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
        final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

        final List<String> list = department.manager().lastName
                .union(primary)
                .list(getEngine(), Option.allowNoTables(true));
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Department department = new Department();
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(0).also(insertTable.text.set("First"))).execute(getEngine());
        final AbstractValueExpressionPrimary<String> primary = insertTable.text.queryValue();

        final List<String> list = primary
                .union(department.manager().lastName)
                .list(getEngine(), Option.allowNoTables(true));
        Collections.sort(list);
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Department department = new Department();
        final List<String> list = createPrimary(department).where(department.deptName.eq("HR")).list(getEngine());
        assertEquals(Arrays.asList("March"), list);
    }

}
