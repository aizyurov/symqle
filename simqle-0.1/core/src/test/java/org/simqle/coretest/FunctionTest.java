package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractRoutineInvocation;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.SqlFunction;
import org.simqle.sql.TableOrView;
import org.simqle.sql.ValueExpression;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class FunctionTest extends SqlTestCase {

    private AbstractRoutineInvocation<String> currentUser() {
        return SqlFunction.create("user", Mappers.STRING).apply();
    }

    private AbstractRoutineInvocation<Long> abs(ValueExpression<Long> e) {

        return SqlFunction.create("abs", Mappers.LONG).apply(e);
    }

    public void testSelectStatementFunctionality() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0", abs(col).show());
    }

    public void testSelectAll() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT ALL abs(T0.id) AS C0 FROM person AS T0", abs(col).all().show());

    }

    public void testSelectDistinct() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT DISTINCT abs(T0.id) AS C0 FROM person AS T0", abs(col).distinct().show());
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = SqlFunction.create("abs", Mappers.LONG) .apply(abs(person.id)).show();
        assertSimilar("SELECT abs(abs(T0.id)) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(abs(id).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id)", sql);
    }

    public void testEq() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).eq(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) = T0.age", sql);
    }

    public void testNe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).ne(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).gt(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) > T0.age", sql);
    }

    public void testGe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).ge(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).lt(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) < T0.age", sql);
    }

    public void testLe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(abs(column).le(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <= T0.age", sql);
    }

    public void testEqValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).eq(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).ne(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).gt(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).ge(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).lt(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(abs(id).le(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <= ?", sql);
    }

    public void testInAll() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id).in(id2.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id).in(abs(id2))).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE abs(T1.id) IN(SELECT abs(T2.id) FROM employee AS T2)", sql);
    }

    public void testNotInAll() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(abs(id).notIn(id2.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) NOT IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        final ValueExpression<Long> expr = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpression<Long> expr2 = DynamicParameter.create(Mappers.LONG, 2L);
        final ValueExpression<Long> expr3 = DynamicParameter.create(Mappers.LONG, 3L);
        String sql = id.where(abs(id).in(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        final ValueExpression<Long> expr = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpression<Long> expr2 = DynamicParameter.create(Mappers.LONG, 2L);
        final ValueExpression<Long> expr3 = DynamicParameter.create(Mappers.LONG, 3L);
        String sql = id.where(abs(id).notIn(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(abs(age).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.age) IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(abs(age).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.age) IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).orderBy(abs(age)).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 ORDER BY abs(T0.age)", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(abs(age).asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) ASC", sql);
    }

    public void testOpposite() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).opposite().show();
        assertSimilar("SELECT - abs(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).pair(age).show();
        assertSimilar("SELECT abs(T0.id) AS C0, T0.age AS C1 FROM person AS T0", sql);
    }

    public void testNoArgFunction() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = currentUser().pair(id).show();
        assertSimilar("SELECT user() AS C0, T0.id AS C1 FROM person AS T0", sql);
    }

    public void testPlus() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).plus(age).show();
        assertSimilar("SELECT abs(T0.id) + T0.age AS C0 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).plus(1).show();
        assertSimilar("SELECT abs(T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).minus(age).show();
        assertSimilar("SELECT abs(T0.id) - T0.age AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).minus(2).show();
        assertSimilar("SELECT abs(T0.id) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).mult(age).show();
        assertSimilar("SELECT abs(T0.id) * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).mult(2).show();
        assertSimilar("SELECT abs(T0.id) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).div(age).show();
        assertSimilar("SELECT abs(T0.id) / T0.age AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).div(3).show();
        assertSimilar("SELECT abs(T0.id) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = abs(id).concat(age).show();
        assertSimilar("SELECT abs(T0.id) || T0.age AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = abs(id).concat(" id").show();
        assertSimilar("SELECT abs(T0.id) || ? AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = abs(person.id).union(person2.id).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 UNION SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = abs(person.id).unionAll(person2.id).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 UNION ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = abs(person.id).unionDistinct(person2.id).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = abs(person.id).except(person2.id).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 EXCEPT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = abs(person.id).exceptAll(person2.id).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = abs(person.id).exceptDistinct(person2.id).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = abs(person.id).intersect(person2.id).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 INTERSECT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = abs(person.id).intersectAll(person2.id).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = abs(person.id).intersectDistinct(person2.id).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = abs(person.id).forUpdate().show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = abs(person.id).forReadOnly().show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testExists() throws Exception {
        final String sql = person2.id.where(abs(person.id).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT abs(T1.id) FROM person AS T1)", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = abs(person.id).queryValue().orderBy(person2.age).show();
        assertSimilar("SELECT(SELECT abs(T0.id) FROM person AS T0) AS C0 FROM person AS T1 ORDER BY T1.age", sql);

    }

    public void testLike() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", Mappers.STRING).apply(person.name).like(DynamicParameter.create(Mappers.STRING, "J%"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", Mappers.STRING).apply(person.name).notLike(DynamicParameter.create(Mappers.STRING, "J%"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", Mappers.STRING).apply(person.name).like("J%")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(SqlFunction.create("to_upper", Mappers.STRING).apply(person.name).notLike("J%")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE to_upper(T0.name) NOT LIKE ?", sql);
    }

    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = abs(person.id).show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        final List<Long> list = abs(person.id).list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = abs(person.id).show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        abs(person.id).scroll(datasource, new Callback<Long>() {
            int callCount = 0;

            @Override
            public boolean iterate(final Long aLong) {
                if (callCount++ != 0) {
                    fail("One call expected, actually " + callCount);
                }
                assertEquals(123L, aLong.longValue());
                return true;
            }
        });
        verify(datasource, connection,  statement, resultSet);
    }


    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
    }

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

}
