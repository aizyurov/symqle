package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.InBox;
import org.symqle.common.Mapper;
import org.symqle.common.OutBox;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.sql.AbstractCharacterFactor;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Label;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractCharacterFactorTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class CharacterFactorTest extends AbstractIntegrationTestBase implements  AbstractCharacterFactorTestSet {

    private AbstractCharacterFactor<String> createCharacterFactor(final Employee employee) {
        return employee.firstName.collate(validCollationNameForVarchar());
    }

    @Override
    protected void runTest() throws Throwable {
        if (!getDatabaseName().equals("Apache Derby")) {
            super.runTest();
        }
    }

    @Override
    public void test_add_Number() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("5"))).execute(getEngine());
            final List<Number> list = insertTable.text.collate(validCollationNameForVarchar()).add(2).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(7, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("5"))).execute(getEngine());
            final List<Number> list = insertTable.id.add(insertTable.text.collate(validCollationNameForVarchar())).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(6, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying + numeric
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_add_Term() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("5"))).execute(getEngine());
            final List<Number> list = insertTable.text.collate(validCollationNameForVarchar()).add(insertTable.id).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(6, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying + numeric
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractCharacterFactor<String> characterFactor = employee.department().manager().firstName.collate(validCollationNameForVarchar());
        final List<String> list = employee.lastName.where(employee.firstName.in(Params.p("Bill").asInValueList().append(characterFactor)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Redwood"), list);
    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final AbstractCharacterFactor<String> characterFactor = employee.department().manager().firstName.collate(validCollationNameForVarchar());
        final List<String> list = employee.lastName.where(employee.firstName.in(characterFactor.asInValueList().append(Params.p("Bill"))))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Redwood"), list);
    }

    @Override
    public void test_asPredicate_() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("1"))).execute(getEngine());
        final List<Integer> list = insertTable.id.where(insertTable.text.collate(validCollationNameForVarchar()).asPredicate()).list(getEngine());
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .orderBy(employee.lastName.collate(validCollationNameForVarchar()).asc())
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_avg_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("1"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("2"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("3"))).execute(getEngine());
            final List<Number> list = insertTable.text.collate(validCollationNameForVarchar()).avg().list(getEngine());
            assertEquals(1, list.size());
            assertEquals(2.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function avg(character varying) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar()).cast("CHAR(10)")
                .orderBy(employee.lastName)
                .list(getEngine());
        if (getDatabaseName().equals("MySQL")) {
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } else {
            assertEquals(Arrays.asList("Cooper    ", "First     ", "March     ", "Pedersen  ", "Redwood   "), list);
        }
    }

    @Override
    public void test_charLength_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.collate(validCollationNameForVarchar()).charLength()
                .orderBy(employee.lastName)
                .list(getEngine());
//        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        assertEquals(Arrays.asList(6, 5, 5, 8, 7), list);
    }

    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .collate(validCollationNameForVarchar())
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .compileQuery(getEngine()).list();
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .concat(employee.firstName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("CooperJames", "FirstJames", "MarchBill", "PedersenAlex", "RedwoodMargaret"), list);
    }

    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .concat("=")
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper=", "First=", "March=", "Pedersen=", "Redwood="), list);
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .concat(employee.firstName.collate(validCollationNameForVarchar()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("CooperJames", "FirstJames", "MarchBill", "PedersenAlex", "RedwoodMargaret"), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        Label l = new Label();
        final List<String> list = department.manager().lastName.label(l)
                .where(employee.lastName.collate(validCollationNameForVarchar()).contains("Redwood"))
                .orderBy(l)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.firstName.collate(validCollationNameForVarchar())
                .countDistinct().list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = employee.firstName.collate(validCollationNameForVarchar())
                .count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .orderBy(employee.lastName.collate(validCollationNameForVarchar()).desc())
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood", "Pedersen", "March", "First", "Cooper"), list);
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                .distinct().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex", "Bill", "James", "Margaret"), list);
    }

    @Override
    public void test_div_Factor() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("6"))).execute(getEngine());
            final List<Number> list = insertTable.text.collate(validCollationNameForVarchar()).div(insertTable.id).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(3, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying + numeric
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_div_Number() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("6"))).execute(getEngine());
            final List<Number> list = insertTable.text.collate(validCollationNameForVarchar()).div(2).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(3, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying + numeric
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(6).also(insertTable.text.set("2"))).execute(getEngine());
            final List<Number> list = insertTable.id.div(insertTable.text.collate(validCollationNameForVarchar())).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(3, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying + numeric
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).eq("Margaret"))
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    @Override
    public void test_eq_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).eq(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.department().manager().firstName.eq(employee.firstName.collate(validCollationNameForVarchar())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<String> list = employee.firstName
                    .exceptAll(department.manager().firstName.collate(validCollationNameForVarchar()))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James"), list);
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                    .exceptAll(department.manager().firstName).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James"), list);
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<String> list = employee.firstName
                    .exceptDistinct(department.manager().firstName.collate(validCollationNameForVarchar()))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill"), list);
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                    .exceptDistinct(department.manager().firstName).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill"), list);
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<String> list = employee.firstName
                    .except(department.manager().firstName.collate(validCollationNameForVarchar()))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill"), list);
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        try {
            final Employee employee = new Employee();
            final Department department = new Department();
            final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                    .except(department.manager().firstName).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex", "Bill"), list);
        } catch (SQLException e) {
            // MySQL does not support EXCEPT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final MyDual myDual = new MyDual();
        final Employee employee = new Employee();
        final List<Integer> list = employee.lastName.count()
                .where(myDual.dummy.collate(validCollationNameForVarchar()).exists())
                .list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .forReadOnly()
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .forUpdate()
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).ge("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar())
                        .ge(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_ge_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.department().manager().firstName
                        .ge(employee.firstName.collate(validCollationNameForVarchar())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).gt("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    @Override
    public void test_gt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar())
                        .gt(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(0, list.size());
    }

    @Override
    public void test_gt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.department().manager().firstName
                        .gt(employee.firstName.collate(validCollationNameForVarchar())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).in(department.manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.in("James", "Bill"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.firstName.in(department.manager().firstName.collate(validCollationNameForVarchar())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                    .intersectAll(department.manager().firstName)
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName
                    .intersectAll(department.manager().firstName.collate(validCollationNameForVarchar()))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                    .intersectDistinct(department.manager().firstName)
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName
                    .intersectDistinct(department.manager().firstName.collate(validCollationNameForVarchar()))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                    .intersect(department.manager().firstName)
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName
                    .intersect(department.manager().firstName.collate(validCollationNameForVarchar()))
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James", "Margaret"), list);
        } catch (SQLException e) {
            // mysql: does not support INTERSECT
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("5"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull())).execute(getEngine());
        final List<Integer> list = insertTable.id.where(insertTable.text.collate(validCollationNameForVarchar()).isNotNull())
                .list(getEngine());
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("5"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull())).execute(getEngine());
        final List<Integer> list = insertTable.id.where(insertTable.text.collate(validCollationNameForVarchar()).isNull())
                .list(getEngine());
        assertEquals(Arrays.asList(2), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar()).label(l).where(employee.firstName.eq("James"))
                .orderBy(l).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First"), list);

    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).le("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
    }

    @Override
    public void test_le_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar())
                        .le(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.department().manager().firstName
                        .le(employee.firstName.collate(validCollationNameForVarchar())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).like("Jam%"))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).like(employee.department().manager().firstName))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.collate(validCollationNameForVarchar()).limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.collate(validCollationNameForVarchar()).limit(2, 5).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).lt("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_lt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar())
                        .lt(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.department().manager().firstName
                        .lt(employee.firstName.collate(validCollationNameForVarchar())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(0, list.size());
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<char[]> list = employee.lastName.collate(validCollationNameForVarchar()).map(charArrayMapper)
                .where(employee.firstName.eq("Margaret"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals("Redwood", new String(list.get(0)));
    }

    @Override
    public void test_max_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("a"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("b"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("c"))).execute(getEngine());
            final List<String> list = insertTable.text.collate(validCollationNameForVarchar()).max().list(getEngine());
            assertEquals(Arrays.asList("c"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function max(character varying) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_min_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("a"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("b"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("c"))).execute(getEngine());
            final List<String> list = insertTable.text.collate(validCollationNameForVarchar()).min().list(getEngine());
            assertEquals(Arrays.asList("a"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function max(character varying) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_mult_Factor() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("6"))).execute(getEngine());
            final List<Number> list = insertTable.text.collate(validCollationNameForVarchar()).mult(insertTable.id).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(12, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying + numeric
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_mult_Number() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("6"))).execute(getEngine());
            final List<Number> list = insertTable.text.collate(validCollationNameForVarchar()).mult(2).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(12, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying + numeric
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(6).also(insertTable.text.set("2"))).execute(getEngine());
            final List<Number> list = insertTable.id.mult(insertTable.text.collate(validCollationNameForVarchar())).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(12, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying + numeric
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).ne("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_ne_Predicand() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar())
                        .ne(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName
                        .ne(employee.department().manager().firstName.collate(validCollationNameForVarchar())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).notIn(department.manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.notIn("James", "Bill"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(employee.firstName.notIn(department.manager().firstName.collate(validCollationNameForVarchar())))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).notLike("Jam%"))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.collate(validCollationNameForVarchar()).notLike(employee.department().manager().firstName))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("a"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.setNull())).execute(getEngine());
            final List<Integer> list = insertTable.id
                    .orderBy(insertTable.text.collate(validCollationNameForVarchar()).nullsFirst())
                    .list(getEngine());
            assertEquals(Arrays.asList(3, 2), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("a"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.setNull())).execute(getEngine());
            final List<Integer> list = insertTable.id
                    .orderBy(insertTable.text.collate(validCollationNameForVarchar()).nullsLast())
                    .list(getEngine());
            assertEquals(Arrays.asList(2, 3), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "MySQL");
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("5"))).execute(getEngine());
            final List<String> list = insertTable.text.collate(validCollationNameForVarchar()).opposite().list(getEngine());
            assertEquals(Arrays.asList("-5"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.salary.gt(2000.0)
                .then(employee.lastName)
                .orElse(employee.firstName.collate(validCollationNameForVarchar()))
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("James", "Cooper"),
                Pair.make("First", "First"),
                Pair.make("Bill", "March"),
                Pair.make("Alex", "Pedersen"),
                Pair.make("Redwood", "Redwood")
        ), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq("James"))
                .orderBy(employee.lastName.collate(validCollationNameForVarchar()))
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName.collate(validCollationNameForVarchar())
                .pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("James", "Cooper"),
                Pair.make("James", "First"),
                Pair.make("Bill", "March"),
                Pair.make("Alex", "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.firstName
                .pair(employee.lastName.collate(validCollationNameForVarchar()))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("James", "Cooper"),
                Pair.make("James", "First"),
                Pair.make("Bill", "March"),
                Pair.make("Alex", "Pedersen"),
                Pair.make("Margaret", "Redwood")
        ), list);
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final DynamicParameter<String> param = employee.lastName.collate(validCollationNameForVarchar()).param();
        param.setValue(" ==");
        final List<String> list = employee.lastName.concat(param).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(Arrays.asList("First =="), list);
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final DynamicParameter<String> param = employee.lastName.collate(validCollationNameForVarchar()).param(" ==");
        final List<String> list = employee.lastName.concat(param).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(Arrays.asList("First =="), list);
    }

    @Override
    public void test_positionOf_String() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = employee.firstName.collate(validCollationNameForVarchar())
                    .positionOf("gar").where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList(4), list);
        } catch (SQLException e) {
            //org.postgresql.util.PSQLException: ERROR: syntax error at or near "COLLATE"
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = employee.firstName.collate(validCollationNameForVarchar())
                    .positionOf(employee.firstName.substring(4, 3))
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList(4), list);
        } catch (SQLException e) {
            //org.postgresql.util.PSQLException: ERROR: syntax error at or near "COLLATE"
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = employee.lastName.concat(employee.firstName)
                    .positionOf(employee.firstName.collate(validCollationNameForVarchar()))
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList(8), list);
        } catch (SQLException e) {
            //org.postgresql.util.PSQLException: ERROR: syntax error at or near "COLLATE"
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_queryValue_() throws Exception {
        MyDual dual = new MyDual();
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = dual.dummy.collate(validCollationNameForVarchar()).queryValue().pair(employee.lastName)
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("X", "Cooper"), Pair.make("X", "First"), Pair.make("X", "March"), Pair.make("X", "Pedersen"), Pair.make("X", "Redwood")), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final Set<String> names = new HashSet<>(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"));
        final int iterations = employee.lastName.collate(validCollationNameForVarchar())
                .scroll(getEngine(), new Callback<String>() {
                    @Override
                    public boolean iterate(final String s) throws SQLException {
                        assertTrue(names.toString(), names.remove(s));
                        return true;
                    }
                });
        assertEquals(5, iterations);
    }

    @Override
    public void test_selectAll_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar()).selectAll().list(getEngine());
        assertEquals(5, list.size());
        assertTrue(list.toString(), list.contains("Cooper"));
        assertTrue(list.toString(), list.contains("Redwood"));
        assertTrue(list.toString(), list.contains("March"));
        assertTrue(list.toString(), list.contains("First"));
        assertTrue(list.toString(), list.contains("Pedersen"));
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one"))).execute(getEngine());
        insertTable.update(insertTable.text.set(insertTable.text.concat("=").collate(validCollationNameForVarchar())))
                .execute(getEngine());
        final List<String> list = insertTable.text.list(getEngine());
        assertEquals(Arrays.asList("one="), list);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = employee.lastName.collate("default").showQuery(new GenericDialect());
        Pattern pattern = Pattern.compile("SELECT ([A-Z][A-Z0-9]+).last_name COLLATE default AS [A-Z][A-Z0-9]+ FROM employee AS \\1");
        assertTrue(sql, pattern.matcher(sql).matches());
    }

    @Override
    public void test_sub_Number() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("5"))).execute(getEngine());
            final List<Number> list = insertTable.text.collate(validCollationNameForVarchar()).sub(2).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(3, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(10).also(insertTable.text.set("6"))).execute(getEngine());
            final List<Number> list = insertTable.id.sub(insertTable.text.collate(validCollationNameForVarchar())).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(4, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying + numeric
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_sub_Term() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("5"))).execute(getEngine());
            final List<Number> list = insertTable.text.collate(validCollationNameForVarchar()).sub(insertTable.id).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(4, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying + numeric
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                    .substring(employee.firstName.charLength().div(2))
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList("garet"), list);
        } catch (SQLException e) {
            // ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                    .substring(employee.firstName.charLength().div(2), employee.firstName.charLength().div(2))
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList("gare"), list);
        } catch (SQLException e) {
            // ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("abcd"))).execute(getEngine());
            final List<String> list = insertTable.text
                    .substring(insertTable.id.collate(validCollationNameForNumber()))
                    .list(getEngine());
            assertEquals(Arrays.asList("bcd"), list);
        } catch (SQLException e) {
            // ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("abcd"))).execute(getEngine());
            final List<String> list = insertTable.text
                    .substring(insertTable.id.collate(validCollationNameForNumber()),
                            insertTable.id.collate(validCollationNameForNumber()))
                    .list(getEngine());
            assertEquals(Arrays.asList("bc"), list);
        } catch (SQLException e) {
            // ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("abcd"))).execute(getEngine());
            final List<String> list = insertTable.text
                    .substring(insertTable.id.collate(validCollationNameForNumber()),
                            insertTable.id.collate(validCollationNameForNumber()))
                    .list(getEngine());
            assertEquals(Arrays.asList("bc"), list);
        } catch (SQLException e) {
            // ERROR: function pg_catalog.substring(character varying, numeric) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_substring_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                .substring(4)
                .where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("garet"), list);
    }

    @Override
    public void test_substring_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                .substring(4, 3)
                .where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("gar"), list);
    }

    @Override
    public void test_sum_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("1"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("2"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("3"))).execute(getEngine());
            final List<Number> list = insertTable.text.collate(validCollationNameForVarchar()).sum().list(getEngine());
            assertEquals(1, list.size());
            assertEquals(6, list.get(0).intValue());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: function sum(character varying) does not exist
            expectSQLException(e, "PostgreSQL");
        }
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = employee.salary.gt(2000.0)
                .then(employee.lastName.collate(validCollationNameForVarchar()))
                .orElse(employee.firstName)
                .pair(employee.lastName)
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("James", "Cooper"),
                Pair.make("First", "First"),
                Pair.make("Bill", "March"),
                Pair.make("Alex", "Pedersen"),
                Pair.make("Redwood", "Redwood")
        ), list);
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .unionAll(department.manager().lastName.collate(validCollationNameForVarchar())).list(getEngine());
        assertEquals(7, list.size());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood", "First", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .unionAll(department.manager().lastName).list(getEngine());
        assertEquals(7, list.size());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood", "First", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .unionDistinct(department.manager().lastName.collate(validCollationNameForVarchar()))
                .list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .unionDistinct(department.manager().lastName)
                .list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .unionDistinct(department.manager().lastName.collate(validCollationNameForVarchar()))
                .list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName.collate(validCollationNameForVarchar())
                .unionDistinct(department.manager().lastName)
                .list(getEngine());
        final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("March", "Pedersen", "First", "Cooper", "Redwood"));
        Collections.sort(expected);
        Collections.sort(list);
        assertEquals(expected, list);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.firstName.collate(validCollationNameForVarchar())
                .where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("Margaret"), list);
    }

    private Mapper<char[]> charArrayMapper = new Mapper<char[]>() {
        @Override
        public char[] value(final InBox inBox) throws SQLException {
            final String string = inBox.getString();
            return string == null ? null : string.toCharArray();
        }

        @Override
        public void setValue(final OutBox outBox, final char[] chars) throws SQLException {
            if (chars == null) {
                outBox.setString(null);
            } else {
                outBox.setString(new String(chars));
            }
        }
    };
}
