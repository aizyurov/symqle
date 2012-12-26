package org.simqle.sql;

import org.simqle.Callback;
import org.simqle.Element;

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

    public static class Concat extends SqlFunction<String> {

        public Concat() {
            super("concat");
        }

        public RoutineInvocation<String> apply(final ValueExpression<?> arg1, final ValueExpression<?> arg2) {
            return super.apply(arg1, arg2);
        }

        @Override
        public String value(final Element element) throws SQLException {
            return element.getString();
        }
    }

    private AbstractRoutineInvocation<Long> abs(ValueExpression<Long> e) {
        return new SqlFunction<Long>("abs"){

            public AbstractRoutineInvocation<Long> apply(ValueExpression<?> arg) {
                return super.apply(arg);
            }

            @Override
            public Long value(Element element) throws SQLException {
                return element.getLong();
            }
        }.apply(e);
    }

    public void testSelectStatementFunctionality() throws Exception {
        final LongColumn col = createId();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0", abs(col).select().show());
    }

    public void testSelectAll() throws Exception {
        final LongColumn col = createId();
        assertSimilar("SELECT ALL abs(T0.id) AS C0 FROM person AS T0", abs(col).all().show());

    }

    public void testSelectDistinct() throws Exception {
        final LongColumn col = createId();
        assertSimilar("SELECT DISTINCT abs(T0.id) AS C0 FROM person AS T0", abs(col).distinct().show());
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = new SqlFunction<Long>("abs") {
            @Override
            public Long value(final Element element) throws SQLException {
                return element.getLong();
            }
        }.apply(abs(createId())).select().show();
        assertSimilar("SELECT abs(abs(T0.id)) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final LongColumn id = createId();
        final String sql = id.where(abs(id).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id)", sql);
    }

    public void testEq() throws Exception {
        final LongColumn id = createId();
        final LongColumn age = createAge();
        final String sql = id.where(abs(id).eq(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) = T0.age", sql);
    }

    public void testNe() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(abs(column).ne(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(abs(column).gt(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) > T0.age", sql);
    }

    public void testGe() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(abs(column).ge(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(abs(column).lt(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) < T0.age", sql);
    }

    public void testLe() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(abs(column).le(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) <= T0.age", sql);
    }

    public void testInAll() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old
        final LongColumn id2 = new LongColumn("id", employee);
        String sql = id.where(abs(id).in(id2.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old
        final LongColumn id2 = new LongColumn("id", employee);
        String sql = id.where(abs(id).in(abs(id2).select())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE abs(T1.id) IN(SELECT abs(T2.id) FROM employee AS T2)", sql);
    }

    public void testNotInAll() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old
        final LongColumn id2 = new LongColumn("id", employee);
        String sql = id.where(abs(id).notIn(id2.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) NOT IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old

        final ValueExpression<Long> expr = new LongParameter(1L);
        final ValueExpression<Long> expr2 = new LongParameter(2L);
        final ValueExpression<Long> expr3 = new LongParameter(3L);
        String sql = id.where(abs(id).in(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final LongColumn id  =  createId();
        // find all but the most old

        final ValueExpression<Long> expr = new LongParameter(1L);
        final ValueExpression<Long> expr2 = new LongParameter(2L);
        final ValueExpression<Long> expr3 = new LongParameter(3L);
        String sql = id.where(abs(id).notIn(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.id) NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.where(abs(age).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.age) IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.where(abs(age).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE abs(T0.age) IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = abs(id).select().orderBy(abs(age)).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0 ORDER BY abs(T0.age)", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.select().orderBy(abs(age).nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.select().orderBy(abs(age).nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.select().orderBy(abs(age).desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = id.select().orderBy(abs(age).asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY abs(T0.age) ASC", sql);
    }

    public void testOpposite() throws Exception {
        final LongColumn id  =  createId();
        String sql = abs(id).opposite().select().show();
        assertSimilar("SELECT - abs(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testPlus() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = abs(id).plus(age).select().show();
        assertSimilar("SELECT abs(T0.id) + T0.age AS C0 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        final LongColumn id  =  createId();
        String sql = abs(id).plus(1).select().show();
        assertSimilar("SELECT abs(T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = abs(id).minus(age).select().show();
        assertSimilar("SELECT abs(T0.id) - T0.age AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        final LongColumn id  =  createId();
        String sql = abs(id).minus(2).select().show();
        assertSimilar("SELECT abs(T0.id) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = abs(id).mult(age).select().show();
        assertSimilar("SELECT abs(T0.id) * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final LongColumn id  =  createId();
        String sql = abs(id).mult(2).select().show();
        assertSimilar("SELECT abs(T0.id) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = abs(id).div(age).select().show();
        assertSimilar("SELECT abs(T0.id) / T0.age AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final LongColumn id  =  createId();
        String sql = abs(id).div(3).select().show();
        assertSimilar("SELECT abs(T0.id) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        String sql = abs(id).concat(age).select().show();
        assertSimilar("SELECT abs(T0.id) || T0.age AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final LongColumn id  =  createId();
        String sql = abs(id).concat(" id").select().show();
        assertSimilar("SELECT abs(T0.id) || ? AS C0 FROM person AS T0", sql);
    }

    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = abs(createId()).select().show();
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

        final List<Long> list = abs(createId()).select().list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = abs(createId()).select().show();
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

        abs(createId()).select().scroll(datasource, new Callback<Long, SQLException>() {
            int callCount = 0;

            @Override
            public void iterate(final Long aLong) throws SQLException, BreakException {
                if (callCount++ != 0) {
                    fail("One call expected, actually " + callCount);
                }
                assertEquals(123L, aLong.longValue());
            }
        });
        verify(datasource, connection,  statement, resultSet);
    }


    private static class Person extends Table {
        private Person() {
            super("person");
        }
    }

    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
    }

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

    private LongColumn createId() {
        return new LongColumn("id", person);
    }

    private LongColumn createAge() {
        return new LongColumn("age", person);
    }

}
