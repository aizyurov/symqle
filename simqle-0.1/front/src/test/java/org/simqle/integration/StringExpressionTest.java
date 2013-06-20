package org.simqle.integration;

import junit.framework.AssertionFailedError;
import org.simqle.Mappers;
import org.simqle.Pair;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.integration.model.MyDual;
import org.simqle.integration.model.One;
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

    private final List<String> caseInsensitiveLikeDatabases = Arrays.asList("mysql");

    private AbstractStringExpression<String> stringExpression(final Employee employee) {
        return employee.firstName.concat(", my friend");
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    public void testMap() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee).map(Mappers.STRING).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee)
                .all().orderBy(employee.firstName)
                .list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    public void testDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = stringExpression(employee)
                    .distinct()
                    .list(getDatabaseGate());
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
        final List<String> list = stringExpression(employee).forUpdate().list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = stringExpression(employee).forReadOnly().list(getDatabaseGate());
            Collections.sort(list);
            assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
        } catch (SQLException e) {
            if (MysqlDialect.class.equals(getDatabaseGate().getDialect().getClass())) {
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
       final List<String> list = stringExpression(employee).orderBy(employee.firstName).list(getDatabaseGate());
       assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, my friend", "James, my friend", "Margaret, my friend"), list);
   }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = stringExpression(employee)
                .where(employee.firstName.ge("James"))
                .orderBy(employee.firstName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("James, my friend", "James, my friend", "Margaret, my friend"), list);
    }

    public void testUnionAll() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<String> list = stringExpression(employee).unionAll(department.manager().firstName.concat(", manager")).list(getDatabaseGate());
        Collections.sort(list);
        assertEquals(Arrays.asList("Alex, my friend", "Bill, my friend", "James, manager", "James, my friend", "James, my friend", "Margaret, manager", "Margaret, my friend"), list);
    }

    public void testUnionDistinct() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        try {
            final List<String> list = stringExpression(employee).unionDistinct(department.manager().firstName.concat(", manager")).list(getDatabaseGate());
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
            final List<String> list = stringExpression(employee).union(department.manager().firstName.concat(", manager")).list(getDatabaseGate());
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
            final List<String> list = stringExpression(employee).intersectAll(department.manager().firstName.concat(", my friend")).list(getDatabaseGate());
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
            final List<String> list = stringExpression(employee).intersect(department.manager().firstName.concat(", my friend")).list(getDatabaseGate());
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
            final List<String> list = stringExpression(employee).intersectDistinct(department.manager().firstName.concat(", my friend")).list(getDatabaseGate());
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
            final List<String> list = stringExpression(employee).exceptAll(department.manager().firstName.concat(", my friend")).list(getDatabaseGate());
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
            final List<String> list = stringExpression(employee).exceptDistinct(department.manager().firstName.concat(", my friend")).list(getDatabaseGate());
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
            final List<String> list = stringExpression(employee).except(department.manager().firstName.concat(", my friend")).list(getDatabaseGate());
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
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make("XYZ", "Cooper"), Pair.make("XYZ", "First")), list);
    }

    public void testExists() throws Exception {
        final Department department = new Department();
        final MyDual myDual = new MyDual();
        final AbstractStringExpression<String> stringExpression = myDual.dummy.concat("YZ");
        final List<String> list = department.deptName.where(stringExpression.exists())
                .orderBy(department.deptName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList("DEV", "HR"), list);
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).eq("Margaret, my friend"))
                    .list(getDatabaseGate());
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
                    .list(getDatabaseGate());
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
                    .list(getDatabaseGate());
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
                    .list(getDatabaseGate());
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
                    .list(getDatabaseGate());
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
                    .list(getDatabaseGate());
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
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList(), list);
    }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).isNotNull())
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testIn() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).in(department.manager().firstName.concat(", my friend")))
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
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
                    .list(getDatabaseGate());
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

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).in("Margaret, my friend", "James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
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

    public void testNotInList() throws Exception {
        final Department department = new Department();
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName
                    .where(stringExpression(employee).notIn("Margaret, my friend", "James, my friend"))
                    .orderBy(employee.lastName)
                    .list(getDatabaseGate());
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

    public void testAdd() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.concat("2").add(3).list(getDatabaseGate());
            assertEquals(1, list.size());
            assertEquals(15, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            expectSQLException(e, "derby");
        }
    }

    public void testSub() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.concat("2").sub(3).list(getDatabaseGate());
            assertEquals(1, list.size());
            assertEquals(9, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            expectSQLException(e, "derby");
        }
    }

    public void testMult() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.concat("2").mult(3).list(getDatabaseGate());
            assertEquals(1, list.size());
            assertEquals(36, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            expectSQLException(e, "derby");
        }
    }

    public void testDiv() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.concat("2").div(3).list(getDatabaseGate());
            assertEquals(1, list.size());
            assertEquals(4, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            expectSQLException(e, "derby");
        }
    }

    public void testOpposite() throws Exception {
        final One one = new One();
        try {
            final List<String> list = one.id.concat("2").opposite().list(getDatabaseGate());
            assertEquals(1, list.size());
            assertEquals("-12", list.get(0));
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            expectSQLException(e, "derby");
        }
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final Department department = new Department();
        final List<Pair<String, String>> list = stringExpression(employee).pair(employee.lastName)
                .where(employee.empId.notIn(department.managerId))
                .orderBy(employee.lastName)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(
                Pair.make("James, my friend", "Cooper"),
                Pair.make("Bill, my friend", "March"),
                Pair.make("Alex, my friend", "Pedersen")), list);
    }

    public void testLike() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(stringExpression(employee).like(employee.department().manager().firstName.concat("%")))
                .orderBy(employee.lastName).list(getDatabaseGate());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testNotLike() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(stringExpression(employee).notLike(employee.department().manager().firstName.concat("%")))
                .orderBy(employee.lastName).list(getDatabaseGate());
        // Cooper has no department, so notLike(null) is FALSE!
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    public void testLikeString() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(stringExpression(employee).like("%a%"))
                .orderBy(employee.lastName).list(getDatabaseGate());
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

    public void testNotLikeString() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName
                .where(stringExpression(employee).notLike("%a%"))
                .orderBy(employee.lastName).list(getDatabaseGate());
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

    public void testCount() throws Exception {
        final Employee employee = new Employee();
        final List<Integer> list = stringExpression(employee).count().list(getDatabaseGate());
        assertEquals(Arrays.asList(5), list);
    }

    public void testCountDistinct() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<Integer> list = stringExpression(employee).countDistinct().list(getDatabaseGate());
            assertEquals(Arrays.asList(4), list);
        } catch (SQLException e) {
            // derby: ERROR X0X67: Columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY,
                // GROUP BY, UNION, INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
            expectSQLException(e, "derby");
        }
    }

    public void testMin() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = stringExpression(employee).min().list(getDatabaseGate());
            assertEquals(Arrays.asList("Alex, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y22: Aggregate MIN cannot operate on type LONG VARCHAR.
            expectSQLException(e, "derby");
        }
    }

    public void testMax() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = stringExpression(employee).max().list(getDatabaseGate());
            assertEquals(Arrays.asList("Margaret, my friend"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y22: Aggregate MIN cannot operate on type LONG VARCHAR.
            expectSQLException(e, "derby");
        }
    }

    public void testAvg() throws Exception {
        final One one = new One();
        try {
            final List<Number> list = one.id.concat("2").avg().list(getDatabaseGate());
            assertEquals(1, list.size());
            assertEquals(12, list.get(0).intValue());
        } catch (SQLException e) {
            // derby: ERROR 42846: Cannot convert types 'INTEGER' to 'VARCHAR'.
            expectSQLException(e, "derby");
        }
    }
}
