package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.common.CoreMappers;
import org.symqle.common.Mappers;
import org.symqle.common.Pair;
import org.symqle.sql.Params;
import org.symqle.dialect.MySqlDialect;
import org.symqle.integration.model.Country;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractCastSpecification;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Functions;
import org.symqle.sql.Label;
import org.symqle.testset.DynamicParameterTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * @author lvovich
 */
public class DynamicParameterTest extends AbstractIntegrationTestBase implements DynamicParameterTestSet {

    @Override
    public void test_add_Number() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Number> list = Params.p(1000).add(500).add(employee.salary)
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(3000.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // ERROR 42X35: It is not allowed for both operands of '+' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.add(DynamicParameter.create(Mappers.DOUBLE, 1000.0))
                .where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(4000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_add_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = DynamicParameter.create(Mappers.DOUBLE, 1000.0).add(employee.salary)
                .where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(4000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        final DynamicParameter<String> param = DynamicParameter.create(Mappers.STRING, "Bill");
        final List<String> list = employee.lastName
                .where(employee.firstName.in(employee.department().manager().firstName.asInValueList().append(param)))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Redwood"), list);
    }

    @Override
    public void test_asInValueList_() throws Exception {
        final Employee employee = new Employee();
        final DynamicParameter<String> param = DynamicParameter.create(Mappers.STRING, "Bill");
        final List<String> list = employee.lastName
                .where(employee.firstName.in(param.asInValueList()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March"), list);
    }

    @Override
    public void test_asBoolean_() throws Exception {
        final Employee employee = new Employee();
        final DynamicParameter<Boolean> param = DynamicParameter.create(Mappers.BOOLEAN, true);
        final List<String> list = employee.lastName
                .where(employee.firstName.eq("Margaret").and(param.asBoolean()))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(Params.p("James").asc(), employee.lastName).list(getEngine());
            final List<String> expected = Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood");
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // org.h2.jdbc.JdbcSQLException: Data conversion error converting "James"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_avg_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Number> list = Params.p(1).avg().where(employee.lastName.isNotNull()).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'AVG' operator is not allowed to take a ? parameter as an operand.
            // org.h2.jdbc.JdbcSQLException: SUM or AVG on wrong data type for "AVG(?1)"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = DynamicParameter.create(CoreMappers.INTEGER, 1).cast("DECIMAL(2)")
                    .where(employee.lastName.isNotNull())
                    .list(getEngine());
            assertEquals(Arrays.asList(1, 1, 1, 1, 1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_charLength_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = DynamicParameter.create(CoreMappers.STRING, "abc").charLength()
                    .where(employee.lastName.isNotNull())
                    .list(getEngine());
            assertEquals(Arrays.asList(3, 3, 3, 3, 3), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_collate_String() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = Params.p("Success").collate(validCollationNameForChar()).where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList("Success"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X01: Syntax error: Encountered "COLLATE" at line 1, column 10.
            // org.h2.jdbc.JdbcSQLException: Syntax error in SQL statement
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(CoreMappers.INTEGER, 1)
                    .compileQuery(getEngine(), Option.allowNoTables(true))
                    .list();
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = Params.p("Ms. ").concat(employee.lastName)
                .where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("Ms. Redwood"), list);
    }

    @Override
    public void test_concat_String() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = Params.p("Ms.").concat(" ").concat(employee.lastName)
                    .where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList("Ms. Redwood"), list);
        } catch (SQLException e) {
            // ERROR 42X35: It is not allowed for both operands of '||' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.concat(Params.p(", Margaret"))
                .where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("Redwood, Margaret"), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Country country = new Country();
        try {
            final List<String> list = country.code.where(Params.p("Redwood").contains("Redwood"))
                    .orderBy(country.code)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("FRA", "RUS", "USA"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = Params.p("Oops").countDistinct()
                    .where(employee.firstName.eq("James"))
                    .list(getEngine());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'COUNT' operator is not allowed to take a ? parameter as an operand
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = Params.p("Oops").count()
                    .where(employee.firstName.eq("James"))
                    .list(getEngine());
            assertEquals(Arrays.asList(2), list);
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'COUNT' operator is not allowed to take a ? parameter as an operand
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(Params.p("James").desc(), employee.lastName).list(getEngine());
            final List<String> expected = Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood");
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // org.h2.jdbc.JdbcSQLException: Data conversion error converting "James"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = DynamicParameter.create(CoreMappers.INTEGER, 1)
                    .distinct()
                    .where(employee.firstName.eq("James"))
                    .list(getEngine());
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_div_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = Params.p(6000.0).div(employee.salary).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(2.0, list.get(0).doubleValue());
    }

    @Override
    public void test_div_Number() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Number> list = Params.p(6000.0).div(3000).where(employee.lastName.eq("First")).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(2.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.div(Params.p(2)).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(1500.0, list.get(0).doubleValue());
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").eq("James")).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '=' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_eq_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("Margaret").eq(employee.firstName)).list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    @Override
    public void test_eq_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq(Params.p("Margaret"))).list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName.where(employee.lastName.eq("Redwood"))
                    .exceptAll(Params.p("Redwood"))
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Margaret"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").exceptAll(employee.firstName).list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName.where(employee.lastName.eq("Redwood"))
                    .exceptDistinct(Params.p("Redwood"))
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Margaret"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood")
                    .exceptDistinct(employee.firstName)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support EXCEPT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.firstName.where(employee.lastName.eq("Redwood"))
                    .except(Params.p("Redwood"))
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Margaret"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood")
                    .except(employee.firstName)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support EXCEPT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Country country = new Country();
        try {
            final List<String> list = country.code.where(Params.p("Redwood").exists())
                    .orderBy(country.code)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("FRA", "RUS", "USA"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        try {
            final List<String> list = Params.p("Redwood").forReadOnly().list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_forUpdate_() throws Exception {
        try {
            final List<String> list = Params.p("Redwood").forUpdate().list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // org.h2.jdbc.JdbcSQLException: General error: "java.lang.NullPointerException"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").ge("James"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '>=' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_ge_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(Params.p("James").ge(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Pedersen"), list);
    }

    @Override
    public void test_ge_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.department().manager().firstName.ge(Params.p("James")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").gt("James"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '>' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_gt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("K").gt(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Pedersen"), list);
    }

    @Override
    public void test_gt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.department().manager().firstName.gt(Params.p("K")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Redwood"), list);
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(Params.p("Margaret").in(department.manager().firstName.where(employee.department().deptId.eq(department.deptId))))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("March", "Redwood");
        assertEquals(expected, list);
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("James").in("Bill", "James"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
           // derby: ERROR 42X35: It is not allowed for both operands of 'IN' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(employee.firstName.in(Params.p("James")))
                    .orderBy(employee.lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Cooper", "First"), list);
        } catch (SQLException e) {
           // ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_all_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(employee.firstName.eq(Params.p("James").all()))
                    .orderBy(employee.lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Cooper", "First"), list);
        } catch (SQLException e) {
           // ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_any_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(employee.firstName.ne(Params.p("James").any()))
                    .orderBy(employee.lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
           // ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_some_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(employee.firstName.eq(Params.p("James").some()))
                    .orderBy(employee.lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("Cooper", "First"), list);
        } catch (SQLException e) {
           // ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").intersectAll(department.manager().lastName).list(getEngine(), Option.allowNoTables(true));
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Department department = new Department();
        try {
            final List<String> list = department.manager().lastName.intersectAll(Params.p("Redwood")).list(getEngine(), Option.allowNoTables(true));
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").intersectDistinct(department.manager().lastName).list(getEngine(), Option.allowNoTables(true));
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Department department = new Department();
        try {
            final List<String> list = department.manager().lastName.intersectDistinct(Params.p("Redwood")).list(getEngine(), Option.allowNoTables(true));
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support INTERSECT
            // H2: does not support INTERSECT/EXCEPT DISTINCT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").intersect(department.manager().lastName).list(getEngine(), Option.allowNoTables(true));
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support INTERSECT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Department department = new Department();
        try {
            final List<String> list = department.manager().lastName.intersect(Params.p("Redwood")).list(getEngine(), Option.allowNoTables(true));
            assertEquals(new HashSet<String>(Arrays.asList("Redwood")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support INTERSECT
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("James").isNotNull())
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood");
        assertEquals(expected, list);
    }

    @Override
    public void test_isNull_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("James").isNull()).list(getEngine());
        assertEquals(0, list.size());
    }

    @Override
    public void test_label_Label() throws Exception {
        try {
            final Employee employee = new Employee();
            final Label l = new Label();
            final List<String> list = Params.p("abc").label(l).where(employee.lastName.eq("First"))
                    .orderBy(l)
                    .list(getEngine());
            assertEquals(Arrays.asList("abc"), list);
        } catch (SQLException e) {
            // Apache Derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("James").le("Margaret"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '<=' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_le_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(Params.p("Margaret").le(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Redwood"), list);
    }

    @Override
    public void test_le_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.department().manager().firstName.le(Params.p("Margaret")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("James").like("%es"))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood");
        assertEquals(expected, list);
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("James").like(employee.firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("Cooper", "First");
        assertEquals(expected, list);
    }

    @Override
    public void test_limit_int() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(CoreMappers.INTEGER, 1)
                    .limit(1)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_limit_int_int() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(CoreMappers.INTEGER, 1)
                    .limit(0, 1)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(CoreMappers.INTEGER, 1).list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_countRows_() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(CoreMappers.INTEGER, 1).countRows().list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }

    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").lt("James")).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '<' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_lt_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(Params.p("K").lt(employee.department().manager().firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Redwood"), list);
    }

    @Override
    public void test_lt_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.department().manager().firstName.lt(Params.p("K")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Pedersen"), list);
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = Params.p(2L).map(Mappers.INTEGER).mult(employee.salary).where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(1, list.size());
        assertEquals(6000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = Params.p(1).max()
                    .where(employee.firstName.eq("James"))
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'MAX' operator is not allowed to take a ? parameter as an operand.
            // org.h2.jdbc.JdbcSQLException: General error: "java.lang.RuntimeException: type=-1"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = Params.p(1).min()
                    .where(employee.firstName.eq("James"))
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'MAX' operator is not allowed to take a ? parameter as an operand.
            // org.h2.jdbc.JdbcSQLException: General error: "java.lang.RuntimeException: type=-1"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_mult_Factor() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = Params.p(2).mult(employee.salary).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(6000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_mult_Number() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Number> list = Params.p(2).mult(2).mult(employee.salary).where(employee.lastName.eq("First")).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(12000.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // ERROR 42X35: It is not allowed for both operands of '*' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.mult(Params.p(2)).where(employee.lastName.eq("First")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(6000.0, list.get(0).doubleValue());
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("Margaret").ne("James"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X35: It is not allowed for both operands of '<>' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_ne_Predicand2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(Params.p("Margaret").ne(employee.firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
    }

    @Override
    public void test_ne_Predicand_Predicand2_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(employee.firstName.ne(Params.p("Margaret")))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = employee.lastName
                .where(Params.p("James").notIn(
                        department.manager().firstName.where(employee.department().deptId.eq(department.deptId))))
                .orderBy(employee.lastName)
                .list(getEngine());
        final List<String> expected = Arrays.asList("Cooper", "March", "Redwood");
        assertEquals(expected, list);
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(Params.p("James").notIn("Bill", "Alex"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
           // derby: ERROR 42X35: It is not allowed for both operands of 'IN' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(employee.firstName.notIn(Params.p("James")))
                    .orderBy(employee.lastName)
                    .list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
           // ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("Bill").notLike("%es"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(Params.p("Bill").notLike(employee.firstName))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(Params.p("James").nullsFirst(), employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support NULLS FIRST
            // org.h2.jdbc.JdbcSQLException: Data conversion error converting "James"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(Params.p("James").nullsLast(), employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            // mysql: does not support NULLS FIRST
            // org.h2.jdbc.JdbcSQLException: Data conversion error converting "James"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL, SupportedDb.H2);
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = Params.p(1).opposite().where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList(-1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);

        }
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        final Employee employee = new Employee();
        Label l = new Label();
        final List<String> list = employee.salary.gt(2000.0).then(employee.lastName).orElse(Params.p("nobody")).label(l)
                .where(employee.firstName.eq("James"))
                .orderBy(l).list(getEngine());
        assertEquals(Arrays.asList("First", "nobody"), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.where(employee.firstName.eq("James")).orderBy(Params.p(1), employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First"), list);
        } catch (SQLException e) {
            // ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Integer> list = Params.p(1).orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList(1, 1, 1, 1, 1), list);
        } catch (SQLException e) {
            // ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Pair<Integer, String>> redwood = Params.p(1).pair(employee.firstName).where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList(Pair.make(1, "Margaret")), redwood);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Pair<String, Integer>> redwood = employee.firstName.pair(Params.p(1)).where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList(Pair.make("Margaret", 1)), redwood);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_param_() throws Exception {
        final DynamicParameter<String> param = Params.p("Bill").param();
        param.setValue("Margaret");
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    @Override
    public void test_param_Object() throws Exception {
        final DynamicParameter<String> param = Params.p("Bill").param("Margaret");
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.firstName.eq(param)).list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    @Override
    public void test_positionOf_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(employee.lastName.positionOf("edwood").eq(Params.p("abcd").positionOf("bc")))
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);

    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(Params.p(2).eq(Params.p(".Redwood").positionOf(employee.lastName)))
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(Params.p(2).eq(employee.lastName.positionOf(Params.p("edwood"))))
                .list(getEngine());
        assertEquals(Arrays.asList("Redwood"), list);
    }

    @Override
    public void test_queryValue_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Pair<String, String>> list = Params.p("X").queryValue().pair(employee.lastName)
                    .orderBy(employee.lastName).list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList(Pair.make("X", "Cooper"), Pair.make("X", "First"), Pair.make("X", "March"), Pair.make("X", "Pedersen"), Pair.make("X", "Redwood")), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        try {
            final int callCount = DynamicParameter.create(CoreMappers.INTEGER, 1).scroll(getEngine(),
                    new Callback<Integer>() {
                        @Override
                        public boolean iterate(final Integer integer) throws SQLException {
                            assertEquals(1, integer.intValue());
                            return true;
                        }
                    }, Option.allowNoTables(true));
            assertEquals(1, callCount);

        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_selectAll_() throws Exception {
        try {
            final List<Integer> list = DynamicParameter.create(CoreMappers.INTEGER, 1)
                    .selectAll().list(getEngine(), Option.allowNoTables(true));
            assertEquals(Arrays.asList(1), list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(
                insertTable.id.set(insertTable.id.param(1))
                        .also(insertTable.text.set(insertTable.text.param("boo"))))
                .execute(getEngine());
        assertEquals(Arrays.asList("boo"), insertTable.text.list(getEngine()));
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final String sql = DynamicParameter.create(Mappers.INTEGER).showQuery(new MySqlDialect(), Option.allowNoTables(true));
        assertEquals("SELECT ? AS C0", sql);
    }

    @Override
    public void test_sub_Number() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<Number> list = Params.p(1000).sub(500).add(employee.salary)
                    .where(employee.lastName.eq("Cooper")).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(2000.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // ERROR 42X35: It is not allowed for both operands of '-' to be ? parameters.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = employee.salary.sub(Params.p(1000))
                .where(employee.lastName.eq("Cooper")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(500.0, list.get(0).doubleValue());
    }

    @Override
    public void test_sub_Term() throws Exception {
        final Employee employee = new Employee();
        final List<Number> list = Params.p(2000).sub(employee.salary)
                .where(employee.lastName.eq("Cooper")).list(getEngine());
        assertEquals(1, list.size());
        assertEquals(500.0, list.get(0).doubleValue());
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = Params.p("abcdefg").substring(employee.firstName.charLength())
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList("efg"), list);
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        final Employee employee = new Employee();
        final AbstractCastSpecification<Number> limit;
        if (getDatabaseName().equals(SupportedDb.MYSQL)) {
            limit = employee.lastName.charLength().div(3).cast("DECIMAL(3)");
        } else {
            limit = employee.lastName.charLength().div(3).cast("INTEGER");
        }

        final List<String> list = Params.p("abcdefg").substring(employee.firstName.charLength(), limit)
                .where(employee.lastName.eq("Cooper"))
                .list(getEngine());
        assertEquals(Arrays.asList("ef"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.substring(Params.p(3)).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("dwood"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.substring(Params.p(3), Params.p(2)).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("dw"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.substring(Params.p(3), Params.p(2)).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList("dw"), list);
    }

    @Override
    public void test_substring_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = Params.p("abcdefg").substring(5)
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList("efg"), list);
    }

    @Override
    public void test_substring_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = Params.p("abcdefg").substring(5, 2)
                .where(employee.lastName.eq("First"))
                .list(getEngine());
        assertEquals(Arrays.asList("ef"), list);
    }

    @Override
    public void test_sum_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Number> list = Params.p(1).sum()
                    .where(employee.firstName.eq("James"))
                    .list(getEngine());
            assertEquals(1, list.size());
            assertEquals(2, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42X36: The 'MAX' operator is not allowed to take a ? parameter as an operand.
            // org.h2.jdbc.JdbcSQLException: SUM or AVG on wrong data type for "AVG(?1)"
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.H2);
        }
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        final Employee employee = new Employee();
        Label l = new Label();
        final List<String> list = employee.salary.lt(2000.0).then(Params.p("nobody")).orElse(employee.lastName).label(l)
                .where(employee.firstName.eq("James"))
                .orderBy(l).list(getEngine());
        assertEquals(Arrays.asList("First", "nobody"), list);
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.unionAll(Params.p("Redwood")).list(getEngine(), Option.allowNoTables(true));
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "Redwood", "First", "Cooper", "March", "Pedersen"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").unionAll(employee.lastName).list(getEngine(), Option.allowNoTables(true));
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "Redwood", "First", "Cooper", "March", "Pedersen"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.unionDistinct(Params.p("Redwood")).list(getEngine(), Option.allowNoTables(true));
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "First", "Cooper", "March", "Pedersen"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = Params.p("Redwood").unionDistinct(employee.lastName).list(getEngine(), Option.allowNoTables(true));
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "First", "Cooper", "March", "Pedersen"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = employee.lastName.union(Params.p("Redwood")).list(getEngine(), Option.allowNoTables(true));
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "First", "Cooper", "March", "Pedersen"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = Params.p("Redwood").union(employee.lastName).list(getEngine(), Option.allowNoTables(true));
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("Redwood", "First", "Cooper", "March", "Pedersen"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = Params.p("abc").where(employee.lastName.eq("Redwood")).list(getEngine());
            assertEquals(Arrays.asList("abc"), list);
        } catch (SQLException e) {
            // ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }
    }

    public void testAsFunctionArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String, Double>> list = employee.lastName.pair(Functions.floor(Params.p(1.2))).where(employee.lastName.eq("Redwood")).list(getEngine());
        assertEquals(Arrays.asList(Pair.make("Redwood", 1.0)), list);
    }

    public void testExistsWithCondition() throws Exception {
        final Country country = new Country();
        final Department department = new Department();
        try {
            final List<String> list = country.code.where(Params.p("Redwood").where(department.countryId.eq(country.countryId)).exists()).list(getEngine());
            assertEquals(new HashSet<String>(Arrays.asList("RUS", "USA")), new HashSet<String>(list));
        } catch (SQLException e) {
            // derby: ERROR 42X34: There is a ? parameter in the select list.  This is not allowed.
            expectSQLException(e, SupportedDb.APACHE_DERBY);
        }

    }


    public void testLikeWithNumericArguments() throws Exception {
        // Note: this is a workaround for derby ERROR 42X35: It is not allowed for both operands of '=' to be ? parameters:
        // 'LIKE' accepts 2 '?'s

        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.where(Params.p(11).like(11 + "")).list(getEngine());
            final ArrayList<String> expected = new ArrayList<String>(Arrays.asList("First", "Cooper", "Redwood", "March", "Pedersen"));
            Collections.sort(expected);
            Collections.sort(list);
            assertEquals(expected, list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: integer ~~ character varying
            expectSQLException(e, SupportedDb.POSTGRESQL);
        }
    }

    public void testNotLikeWithNumericArguments() throws Exception {
        // Note: this is a workaround for derbyERROR 42X35: It is not allowed for both operands of '=' to be ? parameters:
        // 'LIKE' accepts 2 '?'s

        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName.where(Params.p(11).notLike(11 + "")).list(getEngine());
            assertEquals(0, list.size());
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: integer ~~ character varying
            expectSQLException(e, SupportedDb.POSTGRESQL);
        }
    }


}
