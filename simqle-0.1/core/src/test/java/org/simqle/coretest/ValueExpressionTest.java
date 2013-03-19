package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractValueExpression;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.TableOrView;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class ValueExpressionTest extends SqlTestCase {

    public void testShow() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testAll() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().all().show();
        assertSimilar("SELECT ALL T0.name = T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testDistinct() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().distinct().show();
        assertSimilar("SELECT DISTINCT T0.name = T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().where(person.married.booleanValue()).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 WHERE T0.married", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().eq(person.married).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) = T0.married AS C0 FROM person AS T0", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().ne(person.married).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) <> T0.married AS C0 FROM person AS T0", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().gt(person.married).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) > T0.married AS C0 FROM person AS T0", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().ge(person.married).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) >= T0.married AS C0 FROM person AS T0", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().lt(person.married).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) < T0.married AS C0 FROM person AS T0", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().le(person.married).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) <= T0.married AS C0 FROM person AS T0", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().eq(true).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) = ? AS C0 FROM person AS T0", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().ne(true).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) <> ? AS C0 FROM person AS T0", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().gt(true).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) > ? AS C0 FROM person AS T0", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().ge(true).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) >= ? AS C0 FROM person AS T0", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().lt(true).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) < ? AS C0 FROM person AS T0", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().le(true).asValue().show();
        assertSimilar("SELECT(T0.name = T0.nick) <= ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick)", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().pair(person.name).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().opposite().show();
        assertSimilar("SELECT -(T0.name = T0.nick) AS C0 FROM person AS T0", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().in(employee.remote)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IN(SELECT T1.remote FROM employee AS T1)", sql);
    }


    public void testNotIn() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().notIn(employee.remote)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) NOT IN(SELECT T1.remote FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().in(person.name.eq("John").asValue(), person.married)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IN(T0.name = ?, T0.married)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().notIn(person.name.eq("John").asValue(), person.married)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) NOT IN(T0.name = ?, T0.married)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IS NOT NULL", sql);
    }

    public void testPlus() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().plus(person.id).show();
        assertSimilar("SELECT(T0.name = T0.nick) + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().minus(person.id).show();
        assertSimilar("SELECT(T0.name = T0.nick) - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().mult(person.id).show();
        assertSimilar("SELECT(T0.name = T0.nick) * T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().div(person.id).show();
        assertSimilar("SELECT(T0.name = T0.nick) / T0.id AS C0 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().plus(2).show();
        assertSimilar("SELECT(T0.name = T0.nick) + ? AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().minus(2).show();
        assertSimilar("SELECT(T0.name = T0.nick) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().mult(2).show();
        assertSimilar("SELECT(T0.name = T0.nick) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().div(2).show();
        assertSimilar("SELECT(T0.name = T0.nick) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().concat(person.name).show();
        assertSimilar("SELECT(T0.name = T0.nick) || T0.name AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().concat(" test").show();
        assertSimilar("SELECT(T0.name = T0.nick) || ? AS C0 FROM person AS T0", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().forReadOnly().show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().forUpdate().show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().orderBy(person.name).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testAsSortSpecification() throws Exception {
        final String sql = person.name.orderBy(person.name.eq(person.nickName).asValue()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick", sql);
    }

    public void testAsc() throws Exception {
        final String sql = person.name.orderBy(person.name.eq(person.nickName).asValue().asc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick ASC", sql);
    }

    public void testDesc() throws Exception {
        final String sql = person.name.orderBy(person.name.eq(person.nickName).asValue().desc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick DESC", sql);
    }

    public void testNullsFirst() throws Exception {
        final String sql = person.name.orderBy(person.name.eq(person.nickName).asValue().nullsFirst()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick NULLS FIRST", sql);
    }

    public void testNullsLast() throws Exception {
        final String sql = person.name.orderBy(person.name.eq(person.nickName).asValue().nullsLast()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick NULLS LAST", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().union(employee.remote).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 UNION SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().unionAll(employee.remote).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 UNION ALL SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().unionDistinct(employee.remote).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }


    public void testExcept() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().except(employee.remote).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 EXCEPT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().exceptAll(employee.remote).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().exceptDistinct(employee.remote).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().intersect(employee.remote).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 INTERSECT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().intersectAll(employee.remote).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().intersectDistinct(employee.remote).show();
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.name.eq(employee.name).asValue().exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.name = T0.name FROM person AS T1)", sql);
    }

    public void testAsInArgument() throws Exception {
        final String sql = employee.id.where(employee.remote.in(person.name.eq(employee.name).asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.remote IN(SELECT T1.name = T0.name FROM person AS T1)", sql);
    }

    public void testAsInListArguments() throws Exception {
        final String sql = person.id.where(person.married.in(person.name.eq(person.nickName).asValue(), person.name.eq("John").asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.married IN(T0.name = T0.nick, T0.name = ?)", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.name.eq(employee.name).asValue().queryValue().where(person.name.eq("John")).show();
        assertSimilar("SELECT(SELECT T0.name = T1.name FROM employee AS T1) AS C0 FROM person AS T0 WHERE T0.name = ?", sql);
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(person.name.eq("John").asValue()).show();
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name = ? END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNull().then(DynamicParameter.create(Mappers.BOOLEAN, false)).orElse(person.name.eq("John").asValue()).show();
        assertSimilar("SELECT CASE WHEN T0.name IS NULL THEN ? ELSE T0.name = ? END AS C0 FROM person AS T0", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().like(DynamicParameter.create(Mappers.STRING, "true"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().notLike(DynamicParameter.create(Mappers.STRING, "true"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().like("true")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().notLike("true")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick NOT LIKE ?", sql);
    }
    
    public void testCount() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().count().show();
        assertSimilar("SELECT COUNT(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().countDistinct().show();
        assertSimilar("SELECT COUNT(DISTINCT T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testSum() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().sum().show();
        assertSimilar("SELECT SUM(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testAvg() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().avg().show();
        assertSimilar("SELECT AVG(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testMin() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().min().show();
        assertSimilar("SELECT MIN(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testMax() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().max().show();
        assertSimilar("SELECT MAX(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    

    public void testList() throws Exception {
        final AbstractValueExpression<Boolean> valueExpression = person.name.eq(person.nickName).asValue();
        final String queryString = valueExpression.show();
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getBoolean(matches("C[0-9]"))).andReturn(true);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        final List<Boolean> list = valueExpression.list(datasource);
        assertEquals(1, list.size());
        assertEquals(Boolean.TRUE, list.get(0));
        verify(datasource, connection, statement, resultSet);
    }

    public void testScroll() throws Exception {
        final AbstractValueExpression<Boolean> valueExpression = person.name.eq(person.nickName).asValue();
        final String queryString = valueExpression.show();
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getBoolean(matches("C[0-9]"))).andReturn(true);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        valueExpression.scroll(datasource, new Callback<Boolean>() {
            private int callCount;

            @Override
            public boolean iterate(final Boolean aBoolean) {
                assertEquals(0, callCount++);
                assertEquals(Boolean.TRUE, aBoolean);
                return true;
            }
        });
        verify(datasource, connection, statement, resultSet);
    }
























    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<String> nickName = defineColumn(Mappers.STRING, "nick");
        public Column<Boolean> married = defineColumn(Mappers.BOOLEAN, "married");
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Boolean> remote = defineColumn(Mappers.BOOLEAN, "remote");
    }

    private static Person person = new Person();
    private static Employee employee = new Employee();

}
