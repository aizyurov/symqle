package org.symqle.integration;

import junit.framework.AssertionFailedError;
import org.symqle.common.Callback;
import org.symqle.common.InBox;
import org.symqle.common.Mapper;
import org.symqle.common.OutBox;
import org.symqle.common.Pair;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.MyDual;
import org.symqle.integration.model.One;
import org.symqle.sql.AbstractStringExpression;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Label;
import org.symqle.sql.Mappers;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractStringExpressionTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class StringExpressionTest extends AbstractIntegrationTestBase implements AbstractStringExpressionTestSet {

    private final List<String> caseInsensitiveLikeDatabases = Arrays.asList("MySQL");

    private AbstractStringExpression<String> stringExpression(final Employee employee) {
        return employee.firstName.concat(", my friend");
    }

    @Override
    public void test_add_Number() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.concat("2").add(3).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(15, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text + numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_add_NumericExpression_Term_1() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.add(one.id.concat("2")).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(13, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text + numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_add_Term() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.concat("2").add(one.id).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(13, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text + numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_append_InValueList_ValueExpression_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName
                    .where(employee.firstName.in(Params.p("Bill").asInValueList()
                            .append(employee.firstName.substring(1, 3).concat("es"))))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March"), list);
        } catch (SQLException e) {
            // Apache Derby : Comparisons between 'VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)'
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_asInValueList_() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.lastName
                    .where(employee.firstName.in(employee.firstName.substring(1, 3).concat("es").asInValueList()
                            .append(Params.p("Bill"))))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March"), list);
        } catch (SQLException e) {
            // Apache Derby : Comparisons between 'VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)'
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_asPredicate_() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("t"))).execute(getEngine());
        final List<Integer> list = insertTable.id.where(insertTable.text.concat("rue").asPredicate()).list(getEngine());
        // MySQL returns empty list
        if (!"MySQL".equals(getDatabaseName())) {
            assertEquals(Arrays.asList(1), list);
        }
    }

    @Override
    public void test_asc_() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.firstName
                    .orderBy(stringExpression(employee).asc())
                    .list(getEngine());
            assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret"), list);
        } catch (SQLException e) {
            // Apache Derby:
            // Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY, GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported
            expectSQLException(e, "Apache Derby");
        }

    }

    @Override
    public void test_avg_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("1"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("2"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("3"))).execute(getEngine());
            final List<Number> list = insertTable.text.concat("2").avg().list(getEngine());
            assertEquals(1, list.size());
            assertEquals(22.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // ERROR 42Y22: Aggregate AVG cannot operate on type LONG VARCHAR.
            // org.postgresql.util.PSQLException: ERROR: function avg(character varying) does not exist
            expectSQLException(e, "PostgreSQL", "Apache Derby");
        }
    }

    @Override
    public void test_cast_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).cast("CHAR(20)").list(getEngine());
        Collections.sort(list);
        final List<String> expected;
        if ("MySQL".equals(getDatabaseName())) {
            // mysql treats CHAR as VARCHAR, does not append blanks
            expected = new ArrayList<>(
                    Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"));
        } else {
            expected = new ArrayList<>(
                    Arrays.asList("Alex, my friend     ", "Bill, my friend     ", "James, my friend    ", "James, my friend    ", "Margaret, my friend "));
        }

        assertEquals(expected, list);
    }

    @Override
    public void test_charLength_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = stringExpression(employee).charLength().where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(Arrays.asList(19), list);
    }

    @Override
    public void test_collate_String() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = stringExpression(employee).collate(validCollationNameForVarchar())
                    .where(employee.lastName.eq("Redwood"))
                    .list(getEngine());
            assertEquals(Arrays.asList("Margaret, my friend"), list);
        } catch (SQLException e) {
            // Apache Derby: ERROR 42X01: Syntax error: Encountered "COLLATE" at line 1, column 35.
            expectSQLException(e, "Apache Derby");
        }

    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).compileQuery(getEngine()).list();
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    @Override
    public void test_concat_CharacterFactor() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).concat(employee.lastName).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friendPedersen", "Bill, my friendMarch", "James, my friendCooper", "James, my friendFirst", "Margaret, my friendRedwood"), list);
    }

    @Override
    public void test_concat_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).concat(".").list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend.", "Bill, my friend.", "James, my friend.", "James, my friend.", "Margaret, my friend."), list);
    }

    @Override
    public void test_concat_StringExpression_CharacterFactor_1() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.concat(stringExpression(employee))
                .where(employee.lastName.eq("Pedersen"))
                .list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("PedersenAlex, my friend"), list);
    }

    @Override
    public void test_contains_Object() throws Exception {
        final Department department = new Department();
        final MyDual myDual = new MyDual();
        final AbstractStringExpression<String> stringExpression = myDual.dummy.concat("YZ");
        try {
            final List<String> list = department.deptName.where(stringExpression.contains("XYZ"))
                    .orderBy(department.deptName)
                    .list(getEngine());
            assertEquals(Arrays.asList("DEV", "HR"), list);
        } catch (SQLException e) {
            // derby: ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_countDistinct_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = stringExpression(employee).countDistinct().list(getEngine());
            assertEquals(Arrays.asList(4), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_count_() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = stringExpression(employee).count().list(getEngine());
        assertEquals(Arrays.asList(5), list);
    }

    @Override
    public void test_desc_() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.firstName
                    .orderBy(stringExpression(employee).desc())
                    .list(getEngine());
            Collections.reverse(list);
            assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret"), list);
        } catch (SQLException e) {
            // Apache Derby:
            // Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY, GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_distinct_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = stringExpression(employee)
                    .distinct()
                    .list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
            // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_div_Factor() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(4))).execute(getEngine());
            final List<Double> list = insertTable.id.concat("2").div(insertTable.payload)
                    .map(Mappers.DOUBLE)
                    .list(getEngine());
            assertEquals(Arrays.asList(3.0), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text / numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_div_Number() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.concat("2").div(3).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(4, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text / numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_div_Term_Factor_1() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(24))).execute(getEngine());
            final List<Double> list = insertTable.payload.div(insertTable.id.concat("2"))
                    .map(Mappers.DOUBLE)
                    .list(getEngine());
            assertEquals(Arrays.asList(2.0), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text / numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_eq_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).eq("Margaret, my friend"))
                    .list(getEngine());
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_eq_Predicand() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee)
                            .eq(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_eq_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()
                            .eq(stringExpression(employee)))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_exceptAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).exceptAll(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: EXCEPT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_exceptAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).exceptAll(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: EXCEPT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).exceptDistinct(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_exceptDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).exceptDistinct(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_except_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).except(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_except_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).except(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_exists_() throws Exception {
        final Department department = new Department();
        final MyDual myDual = new MyDual();
        final AbstractStringExpression<String> stringExpression = myDual.dummy.concat("YZ");
        final List<String> list = department.deptName.where(stringExpression.exists())
                .orderBy(department.deptName)
                .list(getEngine());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).forReadOnly().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).forUpdate().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    @Override
    public void test_ge_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).ge("James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_ge_Predicand() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee)
                            .ge(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_ge_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()
                            .ge(stringExpression(employee)))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_gt_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).gt("James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_gt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee)
                            .gt(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_gt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()
                            .gt(stringExpression(employee)))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_in_InPredicateValue() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).in(stringExpression(department.manager())))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First","Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_in_Object_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).in("Margaret, my friend", "James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First","Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).in(stringExpression(department.manager())))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First","Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_intersectAll_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee)
                    .intersectAll(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            System.out.println(list);
            assertEquals(Arrays.asList("James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_intersectAll_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee)
                    .intersectAll(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            System.out.println(list);
            assertEquals(Arrays.asList("James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee)
                    .intersectDistinct(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_intersectDistinct_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee)
                    .intersectDistinct(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryPrimary() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee)
                    .intersect(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_intersect_QueryTerm_QueryPrimary_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee)
                    .intersect(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    @Override
    public void test_isNotNull_() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("t"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull())).execute(getEngine());
        final List<Integer> list = insertTable.id.where(insertTable.text.concat("r").isNotNull())
                .list(getEngine());
        assertEquals(Arrays.asList(1), list);
    }

    @Override
    public void test_isNull_() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("t"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull())).execute(getEngine());
        final List<Integer> list = insertTable.id.where(insertTable.text.concat("r").isNull())
                .list(getEngine());
        assertEquals(Arrays.asList(2), list);
    }

    @Override
    public void test_label_Label() throws Exception {
        try {
            final Employee employee = new Employee();
            final Label label = new Label();
            final List<String> list = stringExpression(employee).label(label)
                    .where(employee.empId.eq(employee.department().manager().empId))
                    .orderBy(label)
                    .list(getEngine());
            assertEquals(Arrays.asList("James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // Apache Derby:
            // Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY, GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_le_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).le("James"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_le_Predicand() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee)
                            .le(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_le_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()
                            .le(stringExpression(employee)))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_like_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(stringExpression(employee).like("%a%"))
                .orderBy(employee.lastName).list(getEngine());
        try {
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (AssertionFailedError e) {
            if (caseInsensitiveLikeDatabases.contains(getDatabaseName())) {
                assertEquals(Arrays.asList("Cooper", "First", "Pedersen", "Redwood"), list);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void test_like_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(stringExpression(employee).like(employee.department().manager().firstName.concat("%")))
                .orderBy(employee.lastName).list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_limit_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).limit(2).list(getEngine());
        assertEquals(2, list.size());
    }

    @Override
    public void test_limit_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).limit(2, 10).list(getEngine());
        assertEquals(3, list.size());
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    @Override
    public void test_lt_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).lt("James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_lt_Predicand() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee)
                            .lt(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_lt_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()
                            .le(stringExpression(employee)))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_map_Mapper() throws Exception {
        final Employee employee = new Employee();
        final List<char[]> redwood = stringExpression(employee).map(charArrayMapper)
                .where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(1, redwood.size());
        assertEquals("Margaret, my friend", new String(redwood.get(0)));
    }

    @Override
    public void test_max_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = stringExpression(employee).max().list(getEngine());
            assertEquals(Arrays.asList("Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y22: Aggregate MIN cannot operate on type LONG VARCHAR.
            expectSQLException(e, "Apache Derby");
            // workaround: cast to VARCHAR
            final List<String> list = stringExpression(employee).cast("VARCHAR(256)").max().list(getEngine());
            assertEquals(Arrays.asList("Margaret, my friend"), list);
        }
    }

    @Override
    public void test_min_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = stringExpression(employee).min().list(getEngine());
            assertEquals(Arrays.asList("Alex, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y22: Aggregate MIN cannot operate on type LONG VARCHAR.
            expectSQLException(e, "Apache Derby");
            // workaround: cast to VARCHAR
        }
    }

    @Override
    public void test_mult_Factor() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(4))).execute(getEngine());
            final List<Double> list = insertTable.id.concat("2").mult(insertTable.payload)
                    .map(Mappers.DOUBLE)
                    .list(getEngine());
            assertEquals(Arrays.asList(48.0), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text / numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_mult_Number() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.concat("2").mult(3).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(36, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text * numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_mult_Term_Factor_1() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(4))).execute(getEngine());
            final List<Double> list = insertTable.payload.mult(insertTable.id.concat("2"))
                    .map(Mappers.DOUBLE)
                    .list(getEngine());
            assertEquals(Arrays.asList(48.0), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text / numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_ne_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).ne("James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_ne_Predicand() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee)
                            .ne(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_ne_Predicand_Predicand_1() throws Exception {
        final Employee employee = new Employee();
        final Employee cooper = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(cooper).where(cooper.lastName.eq("Cooper")).queryValue()
                            .ne(stringExpression(employee)))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_notIn_InPredicateValue() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).notIn(stringExpression(department.manager())))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_notIn_Object_Object() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).notIn("Margaret, my friend", "James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).notIn(stringExpression(department.manager())))
                    .orderBy(employee.lastName)
                    .list(getEngine());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_notLike_String() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(stringExpression(employee).notLike("%a%"))
                .orderBy(employee.lastName).list(getEngine());
        try {
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (AssertionFailedError e) {
            if (caseInsensitiveLikeDatabases.contains(getDatabaseName())) {
                assertEquals(Arrays.asList("March"), list);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void test_notLike_StringExpression() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(stringExpression(employee).notLike(employee.department().manager().firstName.concat("%")))
                .orderBy(employee.lastName).list(getEngine());
        // Cooper has no department, so notLike(null) is FALSE!
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("a"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull())).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("c"))).execute(getEngine());
            final List<Integer> list = insertTable.id.orderBy(insertTable.text.concat("x").nullsFirst()).list(getEngine());
            assertEquals(Arrays.asList(2, 1, 3), list);
        } catch (SQLException e) {
            // MySQL does not support
            // Apache Derby:
            // Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY, GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported
            expectSQLException(e, "MySQL", "Apache Derby");
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("a"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull())).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("c"))).execute(getEngine());
            final List<Integer> list = insertTable.id.orderBy(insertTable.text.concat("x").nullsLast()).list(getEngine());
            assertEquals(Arrays.asList(1, 3, 2), list);
        } catch (SQLException e) {
            // MySQL does not support
            // Apache Derby:
            // Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY, GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported
            expectSQLException(e, "MySQL", "Apache Derby");
        }
    }

    @Override
    public void test_opposite_() throws Exception {
        final One one = new One();
        try {
            final List<String> list = one.id.concat("2").opposite().list(getEngine());
            assertEquals(1, list.size());
            assertEquals("-12", list.get(0));
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: - text
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_orElse_SearchedWhenClauseBaseList_ElseClause_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("a"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull())).execute(getEngine());
        insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("c"))).execute(getEngine());
        final List<String> list = insertTable.text.isNull().then(Params.p("null")).orElse(insertTable.text.concat("x"))
                .orderBy(insertTable.id)
                .list(getEngine());
        assertEquals(Arrays.asList("ax", "null", "cx"), list);
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        try {
            final Employee employee = new Employee();
            final List<String> list = employee.firstName
                    .orderBy(stringExpression(employee))
                    .list(getEngine());
            assertEquals(Arrays.asList("Alex", "Bill", "James", "James", "Margaret"), list);
        } catch (SQLException e) {
            // Apache Derby:
            // Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY, GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_orderBy_SortSpecification_SortSpecification() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee)
                .where(employee.empId.eq(employee.department().manager().empId))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("James, my friend", "Margaret, my friend"), list);
    }

    @Override
    public void test_pair_SelectList() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Pair<String, String>> list = stringExpression(employee).pair(employee.lastName)
                .where(employee.empId.notIn(department.managerId))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("James, my friend", "Cooper"),
                Pair.make("Bill, my friend", "March"),
                Pair.make("Alex, my friend", "Pedersen")), list);
    }

    @Override
    public void test_pair_SelectList_SelectList_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Pair<String, String>> list = employee.lastName.pair(stringExpression(employee))
                .where(employee.empId.notIn(department.managerId))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(
                Pair.make("Cooper", "James, my friend"),
                Pair.make("March", "Bill, my friend"),
                Pair.make("Pedersen", "Alex, my friend")), list);
    }

    @Override
    public void test_param_() throws Exception {
        final Employee employee = new Employee();
        final AbstractStringExpression<String> stringExpression = stringExpression(employee);
        final DynamicParameter<String> param = stringExpression.param();
        param.setValue("Margaret, my friend");
        try {
            final List<String> list = employee.lastName.where(stringExpression.eq(param)).list(getEngine());
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // Apache Derby : Comparisons between 'VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)'
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_param_Object() throws Exception {
        final Employee employee = new Employee();
        final AbstractStringExpression<String> stringExpression = stringExpression(employee);
        final DynamicParameter<String> param = stringExpression.param("Margaret, my friend");
        try {
            final List<String> list = employee.lastName.where(stringExpression.eq(param)).list(getEngine());
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // Apache Derby : Comparisons between 'VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)'
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_positionOf_String() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = stringExpression(employee).positionOf("gar").where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(Arrays.asList(4), list);
    }

    @Override
    public void test_positionOf_StringExpression() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = stringExpression(employee).positionOf(employee.firstName.substring(3,4)).where(employee.lastName.eq("Redwood"))
                    .list(getEngine());
            assertEquals(Arrays.asList(3), list);
        } catch (SQLException e) {
            // Apache Derby:
            // Caused by: java.lang.NoSuchMethodError: org.apache.derby.iapi.types.ConcatableDataValue.locate(Lorg/apache/derby/iapi/types/StringDataValue;Lorg/apache/derby/iapi/types/NumberDataValue;Lorg/apache/derby/iapi/types/NumberDataValue;)Lorg/apache/derby/iapi/types/NumberDataValue;
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_positionOf_StringExpression_StringExpression_1() throws Exception {
        final Employee employee = new Employee();
        // Margaret: substr(5,2) || substr(4,1) = "arg"
        final List<Integer> list = employee.firstName.positionOf(
                employee.firstName.substring(5, 2).concat(employee.firstName.substring(4, 1)))
                .where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(Arrays.asList(2), list);

    }

    @Override
    public void test_queryValue_() throws Exception {
        final MyDual myDual = new MyDual();
        final AbstractStringExpression<String> stringExpression = myDual.dummy.concat("YZ");
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = stringExpression.queryValue().pair(employee.lastName)
                .where(employee.firstName.eq("James"))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make("XYZ", "Cooper"), Pair.make("XYZ", "First")), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected = new ArrayList<>(
                Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend",
                        "James, my friend", "Margaret, my friend"));
        stringExpression(employee).scroll(getEngine(), new Callback<String>() {
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
        final List<String> list = stringExpression(employee).selectAll().list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    @Override
    public void test_set_ColumnName_ValueExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("a"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("b"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("c"))).execute(getEngine());

        insertTable.update(insertTable.text.set(insertTable.text.concat("x"))).execute(getEngine());

        final List<String> list = insertTable.text.orderBy(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList("ax", "bx", "cx"), list);

    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = stringExpression(employee).showQuery(getEngine().getDialect());
        Pattern expected = Pattern.compile("SELECT ([A-Z][A-Z0-9]*)\\.first_name \\|\\| \\? AS [A-Z][A-Z0-9]* FROM employee AS \\1");
        assertTrue(sql, expected.matcher(sql).matches());
    }

    @Override
    public void test_sub_Number() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.concat("2").sub(3).list(getEngine());
            assertEquals(1, list.size());
            assertEquals(9, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text - numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_sub_NumericExpression_Term_1() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(14))).execute(getEngine());
            final List<Double> list = insertTable.payload.sub(insertTable.id.concat("2"))
                    .map(Mappers.DOUBLE)
                    .list(getEngine());
            assertEquals(Arrays.asList(2.0), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text / numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_sub_Term() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.payload.set(4))).execute(getEngine());
            final List<Double> list = insertTable.id.concat("2").sub(insertTable.payload)
                    .map(Mappers.DOUBLE)
                    .list(getEngine());
            assertEquals(Arrays.asList(8.0), list);
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: text / numeric
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_substring_NumericExpression() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(4)
                .also(insertTable.text.set("abcde"))
                .also(insertTable.payload.set(3))).execute(getEngine());
        final List<String> list = insertTable.text.concat("fghij").substring(insertTable.payload)
                .list(getEngine());
        assertEquals(Arrays.asList("cdefghij"), list);
    }

    @Override
    public void test_substring_NumericExpression_NumericExpression() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(4)
                .also(insertTable.text.set("abcde"))
                .also(insertTable.payload.set(3))).execute(getEngine());
        final List<String> list = insertTable.text.concat("fghij").substring(insertTable.payload, insertTable.id)
                .list(getEngine());
        assertEquals(Arrays.asList("cdef"), list);
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_1() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1)
                    .also(insertTable.text.set("abcdefghijklm"))
                    .also(insertTable.payload.set(2))).execute(getEngine());
            final List<String> list = insertTable.text.substring(insertTable.id.concat(insertTable.payload))
                    .list(getEngine());
            assertEquals(Arrays.asList("lm"), list);
        } catch (SQLException e) {
            // Apache Derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: integer || integer
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_1() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1)
                    .also(insertTable.text.set("abcdefghijklmnopq"))
                    .also(insertTable.payload.set(2))).execute(getEngine());
            final List<String> list = insertTable.text.substring(insertTable.id.concat(insertTable.payload), Params.p(1))
                    .list(getEngine());
            assertEquals(Arrays.asList("l"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: integer || integer
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_substring_StringExpression_NumericExpression_NumericExpression_2() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1)
                    .also(insertTable.text.set("abcdefghijklmnopq"))
                    .also(insertTable.payload.set(2))).execute(getEngine());
            final List<String> list = insertTable.text.substring(Params.p(1), insertTable.id.concat(insertTable.payload))
                    .list(getEngine());
            assertEquals(Arrays.asList("abcdefghijkl"), list);
        } catch (SQLException e) {
            // org.postgresql.util.PSQLException: ERROR: operator does not exist: integer || integer
            expectSQLException(e, "Apache Derby", "PostgreSQL");
        }
    }

    @Override
    public void test_substring_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).substring(4).where(employee.lastName.eq("Redwood")).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("garet, my friend"), list);
    }

    @Override
    public void test_substring_int_int() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).substring(4, 4).where(employee.lastName.eq("Redwood")).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("gare"), list);
    }

    @Override
    public void test_sum_() throws Exception {
        try {
            InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("1"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("2"))).execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("3"))).execute(getEngine());
            final List<Number> list = insertTable.text.concat("2").avg().list(getEngine());
            assertEquals(1, list.size());
            assertEquals(22.0, list.get(0).doubleValue());
        } catch (SQLException e) {
            // ERROR 42Y22: Aggregate AVG cannot operate on type LONG VARCHAR.
            // org.postgresql.util.PSQLException: ERROR: function avg(character varying) does not exist
            expectSQLException(e, "PostgreSQL", "Apache Derby");
        }
    }

    @Override
    public void test_then_BooleanExpression_ValueExpression_1() throws Exception {
        InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("a"))).execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull())).execute(getEngine());
        insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("c"))).execute(getEngine());
        final List<String> list = insertTable.text.isNotNull().then(insertTable.text.concat("x"))
                .orElse(Params.p("null"))
                .orderBy(insertTable.id)
                .list(getEngine());
        assertEquals(Arrays.asList("ax", "null", "cx"), list);
    }

    @Override
    public void test_unionAll_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = stringExpression(employee).unionAll(stringExpression(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "James, my friend", "Margaret, my friend", "Margaret, my friend"), list);
    }

    @Override
    public void test_unionAll_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = stringExpression(employee).unionAll(stringExpression(department.manager())).list(getEngine());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "James, my friend", "Margaret, my friend", "Margaret, my friend"), list);
    }

    @Override
    public void test_unionDistinct_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).unionDistinct(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
            // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_unionDistinct_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).unionDistinct(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
            // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_union_QueryExpressionBodyScalar_QueryTerm_1() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).union(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
            // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_union_QueryTerm() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).union(stringExpression(department.manager())).list(getEngine());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
            // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).where(employee.lastName.eq("Redwood"))
                .list(getEngine());
        assertEquals(Arrays.asList("Margaret, my friend"), list);
    }

    private Mapper<char[]> charArrayMapper = new Mapper<char[]>() {
        @Override
        public char[] value(final InBox inBox) throws SQLException {
            final String s = inBox.getString();
            return s == null ? null : s.toCharArray();
        }

        @Override
        public void setValue(final OutBox param, final char[] value) throws SQLException {
            if (value == null) {
                param.setString(null);
            } else {
                param.setString(new String(value));
            }
        }
    };
}
