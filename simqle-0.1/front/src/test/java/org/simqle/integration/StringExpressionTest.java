package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.integration.model.MyDual;
import org.simqle.mysql.MysqlDialect;
import org.simqle.sql.AbstractStringExpression;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class StringExpressionTest extends AbstractIntegrationTestBase {

    private AbstractStringExpression<String> stringExpression(final Employee employee) {
        return employee.firstName.concat(", my friend");
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee)
                .all().orderBy(employee.firstName)
                .list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    public void testDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = stringExpression(employee)
                    .distinct()
                    .list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
            // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            expectSQLException(e, "derby");
        }
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).forUpdate().list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = stringExpression(employee).forReadOnly().list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            if (MysqlDialect.class.equals(getDialectDataSource().getDialect().getClass())) {
                // should work with MysqlDialect
                throw e;
            } else {
                // mysql does not support FOR READ ONLY natively
                expectSQLException(e, "mysql");
            }
        }
   }

   public void testOrderBy() throws Exception {
       final Employee employee = new Employee();
       final List<String> list = stringExpression(employee).orderBy(employee.firstName).list(getDialectDataSource());
       assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
   }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee)
                .where(employee.firstName.ge("James"))
                .orderBy(employee.firstName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = stringExpression(employee).unionAll(department.manager().firstName.concat(", manager")).list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, manager", "James, my friend", "James, my friend", "Margaret, manager", "Margaret, my friend"), list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).unionDistinct(department.manager().firstName.concat(", manager")).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, manager", "James, my friend", "Margaret, manager", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
            // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            expectSQLException(e, "derby");
        }
    }

    public void testUnion() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).union(department.manager().firstName.concat(", manager")).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, manager", "James, my friend", "Margaret, manager", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
            // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            expectSQLException(e, "derby");
        }
    }

    public void testIntersectAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).intersectAll(department.manager().firstName.concat(", my friend")).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList("James, my friend", "James, my friend", "Margaret, manager", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "derby", "mysql");
        }
    }

    public void testIntersect() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).intersect(department.manager().firstName.concat(", my friend")).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList("James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "derby", "mysql");
        }
    }

    public void testIntersectDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).intersectDistinct(department.manager().firstName.concat(", my friend")).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList("James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "derby", "mysql");
        }
    }

    public void testExceptAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).exceptAll(department.manager().firstName.concat(", my friend")).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "derby", "mysql");
        }
    }

    public void testExceptDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).exceptDistinct(department.manager().firstName.concat(", my friend")).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "derby", "mysql");
        }
    }

    public void testExcept() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).except(department.manager().firstName.concat(", my friend")).list(getDialectDataSource());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            // mysql: INTERSECT not supported
            expectSQLException(e, "derby", "mysql");
        }
    }

    public void testQueryValue() throws Exception {
        final MyDual myDual = new MyDual();
        final AbstractStringExpression<String> stringExpression = myDual.dummy.concat("YZ");
        final Employee employee = new Employee();
        final List<Pair<String, String>> list = stringExpression.queryValue().pair(employee.lastName)
                .where(employee.firstName.eq("James"))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.of("XYZ", "Cooper"), Pair.of("XYZ", "First")), list);
    }

    public void testExists() throws Exception {
        final Department department = new Department();
        final MyDual myDual = new MyDual();
        final AbstractStringExpression<String> stringExpression = myDual.dummy.concat("YZ");
        final List<String> list = department.deptName.where(stringExpression.exists())
                .orderBy(department.deptName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).eq("Margaret, my friend"))
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "derby");
        }
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).ne("James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "derby");
        }
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).gt("James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "derby");
        }
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).ge("James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "derby");
        }
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).lt("James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "derby");
        }
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).le("James"))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "derby");
        }
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).isNull())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList(), list);
    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).isNotNull())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testIn() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).in(department.manager().firstName.concat(", my friend")))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First","Redwood"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "derby");
        }
    }

    public void testNotIn() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).notIn(department.manager().firstName.concat(", my friend")))
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Pedersen"), list);
        } catch (SQLException e) {
            // derby:  ERROR 42818: Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported.
                // Types must be comparable. String types must also have matching collation.
                // If collation does not match, a possible solution is to
                // cast operands to force them to the default collation
                // (e.g. SELECT tablename FROM sys.systables WHERE CAST(tablename AS VARCHAR(128)) = 'T1')
            expectSQLException(e, "derby");
        }
    }

}
