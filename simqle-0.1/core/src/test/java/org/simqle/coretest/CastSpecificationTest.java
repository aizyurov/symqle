package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.jdbc.StatementOption;
import org.simqle.sql.AbstractCastSpecification;
import org.simqle.sql.Column;
import org.simqle.sql.DatabaseGate;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.GenericDialect;
import org.simqle.sql.SqlFunction;
import org.simqle.sql.TableOrView;

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
public class CastSpecificationTest extends SqlTestCase {


    public void testShow() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C1 FROM person AS T1", col.show());
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C1 FROM person AS T1", col.show(GenericDialect.get()));
    }

    public void testMap() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C1 FROM person AS T1", col.map(Mappers.LONG).show());
    }

    public void testSelectStatementFunctionality() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0", col.show());
    }

    public void testSelectAll() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT ALL CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0", col.all().show());

    }

    public void testSelectDistinct() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT DISTINCT CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0", col.distinct().show());
    }

    public void testEscapeSpecialSymbols() {
        assertEquals("abs\\(T0\\.id\\)", escapeSpecialSymbols("abs(T0.id)"));
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = SqlFunction.create("abs", Mappers.LONG).apply(person.id.cast("NUMBER (12,0)")).show();
        assertSimilar("SELECT abs(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testAsFunctionMultipleArguments() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = SqlFunction.create("max", Mappers.LONG).apply(column, column).show();
        assertSimilar("SELECT max(CAST(T1.id AS NUMBER(12,0)), CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = id.where(person.id.booleanValue()).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testEq() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(id.eq(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) = T0.age", sql);
    }

    public void testNe() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.ne(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.gt(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) > T0.age", sql);
    }

    public void testGe() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.ge(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.lt(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) < T0.age", sql);
    }

    public void testLe() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.le(age)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <= T0.age", sql);
    }

    public void testEqValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.eq(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.ne(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <> ?", sql);
    }

    public void testLtValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.lt(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.le(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <= ?", sql);
    }

    public void testGtValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.gt(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.ge(1L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) >= ?", sql);
    }

    public void testExceptAll() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.exceptAll(person2.age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.exceptDistinct(person.age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.except(person2.age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 EXCEPT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.unionAll(person2.age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 UNION ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.unionDistinct(person2.age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.union(person2.age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 UNION SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.intersectAll(person2.age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersectDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.intersectDistinct(person2.age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.intersect(person2.age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testUseSameTableInDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = column.intersectDistinct(age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testSelectForUpdate() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 FOR UPDATE", col.forUpdate().show());
    }

    public void testSelectForReadOnly() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 FOR READ ONLY", col.forReadOnly().show());
    }

    public void testExists() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final AbstractCastSpecification<Long> age2 = person2.age.cast("FLOAT");
        String sql = id.where(age2.exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT CAST(T1.age AS FLOAT) FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final AbstractCastSpecification<Long> age2 = person2.age.cast("FLOAT");
        String sql = id.where(age2.contains(20L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT CAST(T1.age AS FLOAT) FROM person AS T1)", sql);
    }

    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.cast("NUMBER (12,0)").in(id2)).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE CAST(T1.id AS NUMBER(12,0)) IN(SELECT T2.id FROM employee AS T2)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(id.cast("NUMBER (12,0)").in(1L, 2L, 3L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(id.cast("NUMBER (12,0)").notIn(1L, 2L, 3L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(age.cast("NUMBER (12,0)").isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.age AS NUMBER(12,0)) IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(age.cast("NUMBER (12,0)").isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.age AS NUMBER(12,0)) IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = person.id.cast("NUMBER (12,0)").orderBy(age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 ORDER BY T0.age", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER (12,0)").nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER (12,0)").nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER (12,0)").desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER (12,0)").asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) ASC", sql);
    }

    public void testOperation() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER (12,0)");
        final Column<Long> age = person.age;
        String sql = id.mult(age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER (12,0)").pair(age).show();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0, T0.age AS C1 FROM person AS T0", sql);
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

    public void testAdd() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.add(age).show();
        assertSimilar("SELECT T0.id + T0.age AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.add(1).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.sub(age).show();
        assertSimilar("SELECT T0.id - T0.age AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.sub(1.0).show();
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

    public void testCount() throws Exception {
        final String sql = person.id.count().show();
        assertSimilar("SELECT COUNT(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.parentId.countDistinct().show();
        assertSimilar("SELECT COUNT(DISTINCT T0.parent_id) AS C0 FROM person AS T0", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.age.avg().show();
        assertSimilar("SELECT AVG(T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.age.sum().show();
        assertSimilar("SELECT SUM(T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.age.min().show();
        assertSimilar("SELECT MIN(T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.age.max().show();
        assertSimilar("SELECT MAX(T0.age) AS C0 FROM person AS T0", sql);
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
        new Scenario() {
            @Override
            protected void runQuery(final AbstractCastSpecification<Long> column, final DatabaseGate gate) throws SQLException {
                final List<Long> list = column.list(gate);
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();

    }

    public static abstract class Scenario {
        public void play() throws Exception {
            final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
            final String queryString = column.show();
            final DatabaseGate datasource = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            expect(datasource.getDialect()).andReturn(GenericDialect.get());
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

            runQuery(column, datasource);
            verify(datasource, connection, statement, resultSet);

        }

        protected abstract void runQuery(final AbstractCastSpecification<Long> column, final DatabaseGate gate) throws SQLException;
    }

    public void testOptions() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String queryString = column.show();
        final DatabaseGate datasource = createMock(DatabaseGate.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getDialect()).andReturn(GenericDialect.get());
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
