package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.Column;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.SqlFunction;
import org.symqle.sql.TableOrView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
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
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1", col.show(new GenericDialect()));
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1", col.show(new GenericDialect()));
    }

    public void testMap() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1", col.map(Mappers.LONG).show(new GenericDialect()));
    }

    public void testSelectStatementFunctionality() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0", col.show(new GenericDialect()));
    }

    public void testSelectAll() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0", col.all().show(new GenericDialect()));

    }

    public void testSelectDistinct() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT DISTINCT T0.id AS C0 FROM person AS T0", col.distinct().show(new GenericDialect()));
    }

    public void testEscapeSpecialSymbols() {
        assertEquals("abs\\(T0\\.id\\)", escapeSpecialSymbols("abs(T0.id)"));
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = SqlFunction.create("abs", Mappers.LONG).apply(person.id).show(new GenericDialect());
        assertSimilar("SELECT abs(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testAsFunctionMultipleArguments() throws Exception {
        final Column<Long> column = person.id;
        final String sql = SqlFunction.create("max", Mappers.LONG).apply(column, column).show(new GenericDialect());
        assertSimilar("SELECT max(T0.id, T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testEq() throws Exception {
        final Column<Long> id = person.id;
        final Column<Long> age = person.age;
        final String sql = id.where(id.eq(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id = T0.age", sql);
    }

    public void testNe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(column.ne(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(column.gt(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id > T0.age", sql);
    }

    public void testGe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(column.ge(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(column.lt(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id < T0.age", sql);
    }

    public void testLe() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.where(column.le(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <= T0.age", sql);
    }

    public void testEqValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.eq(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id = ?", sql);
    }

    public void testNeValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.ne(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <> ?", sql);
    }

    public void testLtValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.lt(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id < ?", sql);
    }

    public void testLeValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.le(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <= ?", sql);
    }

    public void testGtValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.gt(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id > ?", sql);
    }

    public void testGeValue() throws Exception {
        final Column<Long> id = person.id;
        final String sql = id.where(id.ge(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id >= ?", sql);
    }

    public void testExceptAll() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.exceptAll(person2.age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.exceptDistinct(person.age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.except(person2.age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.unionAll(person2.age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.unionDistinct(person2.age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.union(person2.age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.intersectAll(person2.age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersectDistinct() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.intersectDistinct(person2.age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final Column<Long> column = person.id;
        final String sql = column.intersect(person2.age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testUseSameTableInDistinct() throws Exception {
        final Column<Long> column = person.id;
        final Column<Long> age = person.age;
        final String sql = column.intersectDistinct(age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testSelectForUpdate() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 FOR UPDATE", col.forUpdate().show(new GenericDialect()));
    }

    public void testSelectForReadOnly() throws Exception {
        final Column<Long> col = person.id;
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 FOR READ ONLY", col.forReadOnly().show(new GenericDialect()));
    }

    public void testExists() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> age2 = person2.age;
        String sql = id.where(age2.exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.age FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> age2 = person2.age;
        String sql = id.where(age2.contains(20L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.age FROM person AS T1)", sql);
    }

    public void testExistsWithCondition() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        // find all but the most old
        final Column<Long> age2 = person2.age;
        String sql = id.where(age2.where(age2.gt(age)).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.age FROM person AS T1 WHERE T1.age > T0.age)", sql);

    }

    public void testInAll() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.in(id2.all())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.in(id2)).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id IN(SELECT T2.id FROM employee AS T2)", sql);
    }

    public void testNotInAll() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.notIn(id2.all())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id NOT IN(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(id.in(1L, 2L, 3L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(id.notIn(1L, 2L, 3L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(age.isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.age IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(age.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.age IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age", sql);
    }

    public void testOrderAsc() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.orderAsc().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY C0 ASC", sql);
    }

    public void testOrderDesc() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.orderDesc().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY C0 DESC", sql);
    }

    public void testOrderByTwoColumns() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age, id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age, T0.id", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.asc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age ASC", sql);
    }

    public void testOperation() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.mult(age).show(new GenericDialect());
        assertSimilar("SELECT T0.id * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.pair(age).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.age AS C1 FROM person AS T0", sql);
    }

    public void testFunction() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        SqlFunction<Long> sumOf = SqlFunction.create("SUM_OF", Mappers.LONG);
        String sql = sumOf.apply(id, age).show(new GenericDialect());
        assertSimilar("SELECT SUM_OF(T0.id, T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.opposite().show(new GenericDialect());
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.add(age).show(new GenericDialect());
        assertSimilar("SELECT T0.id + T0.age AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.add(1).show(new GenericDialect());
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.sub(age).show(new GenericDialect());
        assertSimilar("SELECT T0.id - T0.age AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.sub(1.0).show(new GenericDialect());
        assertSimilar("SELECT T0.id - ? AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.mult(2L).show(new GenericDialect());
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.div(age).show(new GenericDialect());
        assertSimilar("SELECT T0.id / T0.age AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.div(3).show(new GenericDialect());
        assertSimilar("SELECT T0.id / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.concat(age).show(new GenericDialect());
        assertSimilar("SELECT T0.id || T0.age AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.concat(" (id)").show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0", sql);
    }

    public void testCount() throws Exception {
        final String sql = person.id.count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.parentId.countDistinct().show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT T0.parent_id) AS C0 FROM person AS T0", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.age.avg().show(new GenericDialect());
        assertSimilar("SELECT AVG(T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.age.sum().show(new GenericDialect());
        assertSimilar("SELECT SUM(T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.age.min().show(new GenericDialect());
        assertSimilar("SELECT MIN(T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.age.max().show(new GenericDialect());
        assertSimilar("SELECT MAX(T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testLeftJoin() throws Exception {
        final Person person1 = new Person();
        final Person person2 = new Person();
        final Column<Long> id1 = person1.id;
        final Column<Long> id2 = person2.id;
        final Column<Long> parentId1 = person1.parentId;
        final Column<Long> age1 = person1.age;
        final Column<Long> age2 = person2.age;
        person1.leftJoin(person2, parentId1.eq(id2));
        // find all people who are older that parent
        final String sql = id1.where(age1.gt(age2)).show(new GenericDialect());
        System.out.println(sql);
        assertSimilar("SELECT T1.id AS C0 FROM person AS T1 LEFT JOIN person AS T2 ON T1.parent_id = T2.id WHERE T1.age > T2.age", sql);

    }

    public void testRightJoin() throws Exception {
        final Person person1 = new Person();
        final Person person2 = new Person();
        final Column<Long> id1 = person1.id;
        final Column<Long> id2 = person2.id;
        final Column<Long> parentId1 = person1.parentId;
        final Column<Long> age1 = person1.age;
        final Column<Long> age2 = person2.age;
        person1.rightJoin(person2, parentId1.eq(id2));
        // find all people who are older that parent
        final String sql = id1.where(age1.gt(age2)).show(new GenericDialect());
        System.out.println(sql);
        assertSimilar("SELECT T1.id AS C0 FROM person AS T1 RIGHT JOIN person AS T2 ON T1.parent_id = T2.id WHERE T1.age > T2.age", sql);

    }

    public void testInnerJoin() throws Exception {
        final Person person1 = new Person();
        final Person person2 = new Person();
        final Column<Long> id1 = person1.id;
        final Column<Long> id2 = person2.id;
        final Column<Long> parentId1 = person1.parentId;
        final Column<Long> age1 = person1.age;
        final Column<Long> age2 = person2.age;
        person1.innerJoin(person2, parentId1.eq(id2));
        // find all people who are older that parent
        final String sql = id1.where(age1.gt(age2)).show(new GenericDialect());
        System.out.println(sql);
        assertSimilar("SELECT T1.id AS C0 FROM person AS T1 INNER JOIN person AS T2 ON T1.parent_id = T2.id WHERE T1.age > T2.age", sql);

    }

    public void testOuterJoin() throws Exception {
        final Person person1 = new Person();
        final Person person2 = new Person();
        final Column<Long> id1 = person1.id;
        final Column<Long> id2 = person2.id;
        final Column<Long> parentId1 = person1.parentId;
        final Column<Long> age1 = person1.age;
        final Column<Long> age2 = person2.age;
        person1.outerJoin(person2, parentId1.eq(id2));
        // find all people who are older that parent
        final String sql = id1.where(age1.gt(age2)).show(new GenericDialect());
        System.out.println(sql);
        assertSimilar("SELECT T1.id AS C0 FROM person AS T1 OUTER JOIN person AS T2 ON T1.parent_id = T2.id WHERE T1.age > T2.age", sql);

    }


    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.name.notLike(DynamicParameter.create(Mappers.STRING, "John%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name NOT LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.name.notLike("John%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name NOT LIKE ?", sql);
    }

    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final Column<Long> column, final DatabaseGate gate) throws SQLException {
                final List<Long> list = column.list(gate);
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();

    }

    public static abstract class Scenario {
        public void play() throws Exception {
            final Column<Long> column = person.id;
            final String queryString = column.show(new GenericDialect());
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
            expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection,  statement, resultSet);

            runQuery(column, gate);
            verify(gate, connection, statement, resultSet);

        }

        protected abstract void runQuery(final Column<Long> column, final DatabaseGate gate) throws SQLException;
    }

    public void testOptions() throws Exception {
        final Column<Long> column = person.id;
        final String queryString = column.show(new GenericDialect());
        final DatabaseGate gate = createMock(DatabaseGate.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
        expect(gate.getDialect()).andReturn(new GenericDialect());
        expect(gate.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setFetchSize(10);
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        statement.setMaxFieldSize(15);
        statement.setMaxRows(5);
        statement.setQueryTimeout(100);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(gate, connection,  statement, resultSet);

        final List<Long> list = column.list(gate, Option.setFetchSize(10),
                Option.setFetchDirection(ResultSet.FETCH_FORWARD),
                Option.setMaxFieldSize(15),
                Option.setMaxRows(5),
                Option.setQueryTimeout(100),
                Option.allowNoTables(false));
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(gate, connection, statement, resultSet);

    }

    public void testGateOptions() throws Exception {
        final Column<Long> column = person.id;
        final String queryString = column.show(new GenericDialect());
        final DatabaseGate gate = createMock(DatabaseGate.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(gate.getOptions()).andReturn(Arrays.<Option>asList(Option.setFetchSize(10),
                Option.setFetchDirection(ResultSet.FETCH_FORWARD),
                Option.setQueryTimeout(100)));
        expect(gate.getDialect()).andReturn(new GenericDialect());
        expect(gate.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setFetchSize(10);
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        statement.setQueryTimeout(100);
        statement.setMaxFieldSize(15);
        statement.setMaxRows(5);
        statement.setQueryTimeout(50);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(gate, connection,  statement, resultSet);

        final List<Long> list = column.list(gate,
                Option.setMaxFieldSize(15),
                Option.setMaxRows(5),
                Option.setQueryTimeout(50));
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(gate, connection, statement, resultSet);

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
