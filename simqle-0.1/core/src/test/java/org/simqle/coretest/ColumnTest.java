package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.jdbc.StatementOption;
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
import java.sql.Statement;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 21.11.12
 * Time: 20:50
 * To change this template use File | Settings | File Templates.
 */
public class ColumnTest extends SqlTestCase {


    public void testShow() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1", col.show());
    }

    public void testSelectStatementFunctionality() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0", col.show());
    }

    public void testSelectAll() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0", col.all().show());

    }

    public void testSelectDistinct() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT DISTINCT T0.id AS C0 FROM person AS T0", col.distinct().show());
    }

    public void testEscapeSpecialSymbols() {
        assertEquals("abs\\(T0\\.id\\)", escapeSpecialSymbols("abs(T0.id)"));
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = SqlFunction.create("abs", Mappers.LONG).apply(person.id).show();
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testAsFunctionMultipleArguments() throws Exception {
        final Column<Long> column = person.id;
        final String sql = SqlFunction.create("max", Mappers.LONG).apply(column, column).show();
        assertSimilar("SELECT max(T0.id, T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testEq() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(id.eq(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id = T0.age", sql);
    }

    public void testNe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(column.ne(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(column.gt(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id > T0.age", sql);
    }

    public void testGe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(column.ge(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(column.lt(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id < T0.age", sql);
    }

    public void testLe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(column.le(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <= T0.age", sql);
    }

    public void testEqValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.eq(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id = ?", sql);
    }

    public void testNeValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.ne(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <> ?", sql);
    }

    public void testLtValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.lt(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id < ?", sql);
    }

    public void testLeValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.le(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <= ?", sql);
    }

    public void testGtValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.gt(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id > ?", sql);
    }

    public void testGeValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.ge(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id >= ?", sql);
    }

    public void testExceptAll() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.exceptAll(person2.age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.exceptDistinct(person.age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.except(person2.age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.unionAll(person2.age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.unionDistinct(person2.age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.union(person2.age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.intersectAll(person2.age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersectDistinct() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.intersectDistinct(person2.age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.intersect(person2.age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testUseSameTableInDistinct() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.intersectDistinct(age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testSelectForUpdate() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 FOR UPDATE", col.forUpdate().show());
    }

    public void testSelectForReadOnly() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 FOR READ ONLY", col.forReadOnly().show());
    }

    public void testExists() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> age2 = person2.age;
        String sql = id.where(age2.exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.age FROM person AS T1)", sql);

    }

    public void testExistsWithCondition() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        // find all but the most old
        final Column<Long> age2 = person2.age;
        String sql = id.where(age2.where(age2.gt(age)).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.age FROM person AS T1 WHERE T1.age > T0.age)", sql);

    }

    public void testInAll() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.in(id2.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.in(id2)).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id IN(SELECT T2.id FROM employee AS T2)", sql);
    }

    public void testNotInAll() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.notIn(id2.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id NOT IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        final ValueExpression<Long> expr = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpression<Long> expr2 = DynamicParameter.create(Mappers.LONG, 2L);
        final ValueExpression<Long> expr3 = DynamicParameter.create(Mappers.LONG, 3L);
        String sql = id.where(id.in(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        final ValueExpression<Long> expr = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpression<Long> expr2 = DynamicParameter.create(Mappers.LONG, 2L);
        final ValueExpression<Long> expr3 = DynamicParameter.create(Mappers.LONG, 3L);
        String sql = id.where(id.notIn(expr, expr2, expr3)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(age.isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.age IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(age.isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.age IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age", sql);
    }

    public void testOrderByTwoColumns() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age, id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age, T0.id", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age ASC", sql);
    }

    public void testOperation() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.mult(age).show();
        assertSimilar("SELECT T0.id * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.pair(age).show();
        assertSimilar("SELECT T0.id AS C0, T0.age AS C1 FROM person AS T0", sql);
    }

    public void testFunction() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        SqlFunction<Long> sumOf = SqlFunction.create("SUM_OF", Mappers.LONG);
        String sql = sumOf.apply(id, age).show();
        assertSimilar("SELECT SUM_OF(T0.id, T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.opposite().show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testPlus() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.plus(age).show();
        assertSimilar("SELECT T0.id + T0.age AS C0 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.plus(1).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.minus(age).show();
        assertSimilar("SELECT T0.id - T0.age AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.minus(1.0).show();
        assertSimilar("SELECT T0.id - ? AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.mult(2L).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.div(age).show();
        assertSimilar("SELECT T0.id / T0.age AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.div(3).show();
        assertSimilar("SELECT T0.id / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.concat(age).show();
        assertSimilar("SELECT T0.id || T0.age AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.concat(" (id)").show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0", sql);
    }

    public void testJoin() throws Exception {
        final Person person1 = new Person();
        final Person person2 = new Person();
        final Column<Long> id1 = person1.id;
        final Column<Long> id2 = person2.id;
        final Column<Long> parentId1 = person1.parentId;
        final Column<Long> age1 = person1.age;
        final Column<Long> age2 = person2.age;
        person1.leftJoin(person2, parentId1.eq(id2));
        // find all people who are older that parent
        final String sql = id1.where(age1.gt(age2)).show();
        System.out.println(sql);
        assertSimilar("SELECT T1.id AS C0 FROM person AS T1 LEFT JOIN person AS T2 ON T1.parent_id = T2.id WHERE T1.age > T2.age", sql);

    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.name.notLike(DynamicParameter.create(Mappers.STRING, "John%"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name NOT LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.name.notLike("John%")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name NOT LIKE ?", sql);
    }

    public void testList() throws Exception {
        final Column<Long> column = person.id;
        final String queryString = column.show();
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
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

        final List<Long> list = column.list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);
    }

    public void testOptions() throws Exception {
        final Column<Long> column = person.id;
        final String queryString = column.show();
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setFetchSize(10);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        final List<Long> list = column.list(datasource, new StatementOption() {
            @Override
            public void apply(final Statement statement) throws SQLException {
                statement.setFetchSize(10);
            }
        });
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);

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
