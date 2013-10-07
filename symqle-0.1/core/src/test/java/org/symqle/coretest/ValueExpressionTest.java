package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.Mappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractAggregateFunction;
import org.symqle.sql.AbstractValueExpression;
import org.symqle.sql.Column;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class ValueExpressionTest extends SqlTestCase {

    public void testShow() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0", sql);
        final String sql2 = person.name.eq(person.nickName).asValue().show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testMap() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().map(Mappers.STRING).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testAll() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().selectAll().show(new GenericDialect());
        assertSimilar("SELECT ALL T0.name = T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testDistinct() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().distinct().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.name = T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().where(person.married.asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 WHERE T0.married", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().eq(person.married).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) = T0.married AS C0 FROM person AS T0", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().ne(person.married).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) <> T0.married AS C0 FROM person AS T0", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().gt(person.married).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) > T0.married AS C0 FROM person AS T0", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().ge(person.married).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) >= T0.married AS C0 FROM person AS T0", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().lt(person.married).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) < T0.married AS C0 FROM person AS T0", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().le(person.married).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) <= T0.married AS C0 FROM person AS T0", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().eq(true).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) = ? AS C0 FROM person AS T0", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().ne(true).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) <> ? AS C0 FROM person AS T0", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().gt(true).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) > ? AS C0 FROM person AS T0", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().ge(true).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) >= ? AS C0 FROM person AS T0", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().lt(true).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) < ? AS C0 FROM person AS T0", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().le(true).asValue().show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) <= ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick)", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().pair(person.name).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().opposite().show(new GenericDialect());
        assertSimilar("SELECT -(T0.name = T0.nick) AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().cast("CHAR(10)").show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.name = T0.nick AS CHAR(10)) AS C0 FROM person AS T0", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().in(employee.remote)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IN(SELECT T1.remote FROM employee AS T1)", sql);
    }


    public void testNotIn() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().notIn(employee.remote)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) NOT IN(SELECT T1.remote FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().in(true, false)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IN(?, ?)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().notIn(true, false)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) NOT IN(?, ?)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name = T0.nick) IS NOT NULL", sql);
    }

    public void testAdd() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().add(person.id).show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().sub(person.id).show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().mult(person.id).show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) * T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().div(person.id).show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) / T0.id AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().add(2).show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) + ? AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().sub(2).show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().mult(2).show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().div(2).show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().concat(person.name).show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) || T0.name AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().concat(" test").show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) || ? AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().substring(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.name = T0.nick) FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().substring(person.id, person.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.name = T0.nick) FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().substring(2).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.name = T0.nick) FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().substring(2, 5).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.name = T0.nick) FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().positionOf(person.married).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.married IN(T0.name = T0.nick)) AS C0 FROM person AS T0", sql);
    }

    public void testParam() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().positionOf("e").show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN(T0.name = T0.nick)) AS C0 FROM person AS T0", sql);
    }


    public void testCollate() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().collate("latin1_general_ci").show(new GenericDialect());
        assertSimilar("SELECT(T0.name = T0.nick) COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().orderBy(person.name).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testOrderAsc() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().orderAsc().show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 ORDER BY C0 ASC", sql);
    }

    public void testOrderDesc() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().orderDesc().show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 ORDER BY C0 DESC", sql);
    }

    public void testAsSortSpecification() throws Exception {
        final String sql = person.name.orderBy(person.name.eq(person.nickName).asValue()).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick", sql);
    }

    public void testAsc() throws Exception {
        final String sql = person.name.orderBy(person.name.eq(person.nickName).asValue().asc()).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick ASC", sql);
    }

    public void testDesc() throws Exception {
        final String sql = person.name.orderBy(person.name.eq(person.nickName).asValue().desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick DESC", sql);
    }

    public void testNullsFirst() throws Exception {
        final String sql = person.name.orderBy(person.name.eq(person.nickName).asValue().nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick NULLS FIRST", sql);
    }

    public void testNullsLast() throws Exception {
        final String sql = person.name.orderBy(person.name.eq(person.nickName).asValue().nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.name = T0.nick NULLS LAST", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().union(employee.remote).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 UNION SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().unionAll(employee.remote).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 UNION ALL SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().unionDistinct(employee.remote).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }


    public void testExcept() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().except(employee.remote).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 EXCEPT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().exceptAll(employee.remote).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().exceptDistinct(employee.remote).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().intersect(employee.remote).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 INTERSECT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().intersectAll(employee.remote).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().intersectDistinct(employee.remote).show(new GenericDialect());
        assertSimilar("SELECT T0.name = T0.nick AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.remote AS C0 FROM employee AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.name.eq(employee.name).asValue().exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.name = T0.name FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = employee.id.where(person.name.eq(employee.name).asValue().contains(true)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT T1.name = T0.name FROM person AS T1)", sql);
    }

    public void testAsInArgument() throws Exception {
        final String sql = employee.id.where(employee.remote.in(person.name.eq(employee.name).asValue())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.remote IN(SELECT T1.name = T0.name FROM person AS T1)", sql);
    }

    public void testAsInListArguments() throws Exception {
        final String sql = person.id.where(person.married.in(true, false)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.married IN(?, ?)", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.name.eq(employee.name).asValue().queryValue().where(person.name.eq("John")).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.name = T1.name FROM employee AS T1) AS C0 FROM person AS T0 WHERE T0.name = ?", sql);
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(person.name.eq("John").asValue()).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name = ? END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNull().then(DynamicParameter.create(Mappers.BOOLEAN, false)).orElse(person.name.eq("John").asValue()).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NULL THEN ? ELSE T0.name = ? END AS C0 FROM person AS T0", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().like(DynamicParameter.create(Mappers.STRING, "true"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().notLike(DynamicParameter.create(Mappers.STRING, "true"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().like("true")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.name.eq(person.nickName).asValue().notLike("true")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name = T0.nick NOT LIKE ?", sql);
    }
    
    public void testCount() throws Exception {
        final AbstractAggregateFunction<Integer> count = person.name.eq(person.nickName).asValue().count();
        final String sql = count.show(new GenericDialect());
        assertSimilar("SELECT COUNT(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().countDistinct().show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testSum() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().sum().show(new GenericDialect());
        assertSimilar("SELECT SUM(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testAvg() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().avg().show(new GenericDialect());
        assertSimilar("SELECT AVG(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testMin() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().min().show(new GenericDialect());
        assertSimilar("SELECT MIN(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    public void testMax() throws Exception {
        final String sql = person.name.eq(person.nickName).asValue().max().show(new GenericDialect());
        assertSimilar("SELECT MAX(T0.name = T0.nick) AS C1 FROM person AS T1", sql);
    }
    
    

    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final AbstractValueExpression<Boolean> valueExpression, final DatabaseGate gate) throws SQLException {
                final List<Boolean> list = valueExpression.list(gate);
                assertEquals(1, list.size());
                assertEquals(Boolean.TRUE, list.get(0));
            }
        }.play();
    }



    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final AbstractValueExpression<Boolean> valueExpression, final DatabaseGate gate) throws SQLException {
                valueExpression.scroll(gate, new Callback<Boolean>() {
                    private int callCount;

                    @Override
                    public boolean iterate(final Boolean aBoolean) {
                        assertEquals(0, callCount++);
                        assertEquals(Boolean.TRUE, aBoolean);
                        return true;
                    }
                });
            }
        }.play();

    }

    private static abstract class Scenario {
        public void play() throws Exception {
            final AbstractValueExpression<Boolean> valueExpression = person.name.eq(person.nickName).asValue();
            final String queryString = valueExpression.show(new GenericDialect());
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(new GenericDialect());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getBoolean(matches("C[0-9]"))).andReturn(true);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection,  statement, resultSet);

            runQuery(valueExpression, gate);
            verify(gate, connection, statement, resultSet);
        }

        protected abstract void runQuery(final AbstractValueExpression<Boolean> valueExpression, final DatabaseGate gate) throws SQLException;
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
