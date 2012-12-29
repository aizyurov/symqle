package org.simqle.sql;

import org.simqle.Callback;

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
public class NullPredicateTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.isNull().and(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.isNull().or(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.isNull().negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive IS NULL", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) IS NOT NULL", sql);
    }


    public void testAndTwoPredicates() throws Exception {
        final String sql = person.id.where(person.alive.eq(person.smart).and(person.friendly.isNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive = T0.smart AND T0.friendly IS NULL", sql);
    }

    public void testOrPredicate() throws Exception {
        final String sql = person.id.where(person.alive.eq(person.smart).or(person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive = T0.smart OR T0.friendly", sql);
    }

    public void testSelect() throws Exception {
        final String sql = person.alive.isNull().show();
        assertSimilar("SELECT T0.alive IS NULL AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.alive.isNull().all().show();
        assertSimilar("SELECT ALL T0.alive IS NULL AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.alive.isNull().distinct().show();
        assertSimilar("SELECT DISTINCT T0.alive IS NULL AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.alive.isNull().where(person.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS NULL AS C0 FROM person AS T0 WHERE T0.smart", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.isNull().eq(person.smart.isNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) =(T0.smart IS NULL)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.isNull().ne(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.isNull().gt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.isNull().ge(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.isNull().lt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) <(T0.smart)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.isNull().le(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) <=(T0.smart)", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.isNull().in(person2.alive.isNull())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart IS NULL) IN(SELECT T2.alive IS NULL FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.isNull().notIn(person2.alive.booleanValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart IS NULL) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.isNull().in(person.alive.isNull(), person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart IS NULL) IN(T0.alive IS NULL, T0.friendly)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.isNull().notIn(person.alive.booleanValue(), person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart IS NULL) NOT IN(T0.alive, T0.friendly)", sql);
   }

    public void testOrderBy() throws Exception {
        String sql = person.alive.isNull().orderBy(person.smart.isNull()).show();
        assertSimilar("SELECT T0.alive IS NULL AS C0 FROM person AS T0 ORDER BY T0.smart IS NULL", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.smart.isNull().nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart IS NULL NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.smart.isNull().nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart IS NULL NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.smart.isNull().desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart IS NULL DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.smart.isNull().asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart IS NULL ASC", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = person.smart.isNull().opposite().show();
        assertSimilar("SELECT -(T0.smart IS NULL) AS C0 FROM person AS T0", sql);
    }


    public void testPlus() throws Exception {
        String sql = person.smart.isNull().plus(person.alive.isNull()).show();
        assertSimilar("SELECT(T0.smart IS NULL) +(T0.alive IS NULL) AS C0 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        String sql = person.smart.isNull().plus(2).show();
        assertSimilar("SELECT(T0.smart IS NULL) + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.smart.isNull().booleanValue().show();
        assertSimilar("SELECT(T0.smart IS NULL) AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        String sql = person.smart.isNull().minus(person.alive.booleanValue()).show();
        assertSimilar("SELECT(T0.smart IS NULL) -(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        String sql = person.smart.isNull().minus(2).show();
        assertSimilar("SELECT(T0.smart IS NULL) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.smart.isNull().mult(person.alive.isNull()).show();
        assertSimilar("SELECT(T0.smart IS NULL) *(T0.alive IS NULL) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.smart.isNull().mult(2).show();
        assertSimilar("SELECT(T0.smart IS NULL) * ? AS C0 FROM person AS T0", sql);
    }
    public void testDiv() throws Exception {
        String sql = person.smart.isNull().div(person.alive.booleanValue()).show();
        assertSimilar("SELECT(T0.smart IS NULL) /(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.smart.isNull().div(2).show();
        assertSimilar("SELECT(T0.smart IS NULL) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.smart.isNull().concat(person.alive.booleanValue()).show();
        assertSimilar("SELECT(T0.smart IS NULL) ||(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = person.smart.isNull().concat(" test").show();
        assertSimilar("SELECT(T0.smart IS NULL) || ? AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.smart.isNull().union(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 UNION SELECT T1.alive AS C0 FROM person AS T1", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = person.smart.isNull().unionAll(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 UNION ALL SELECT T1.alive AS C0 FROM person AS T1", sql);

    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.smart.isNull().unionDistinct(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.alive AS C0 FROM person AS T1", sql);

    }

    public void testExcept() throws Exception {
        final String sql = person.smart.isNull().except(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 EXCEPT SELECT T1.alive AS C0 FROM person AS T1", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = person.smart.isNull().exceptAll(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.alive AS C0 FROM person AS T1", sql);

    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.smart.isNull().exceptDistinct(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.alive AS C0 FROM person AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = person.smart.isNull().intersect(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 INTERSECT SELECT T1.alive AS C0 FROM person AS T1", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = person.smart.isNull().intersectAll(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.alive AS C0 FROM person AS T1", sql);

    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.smart.isNull().intersectDistinct(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.alive AS C0 FROM person AS T1", sql);

    }

    public void testForUpdate() throws Exception {
        final String sql = person.smart.isNull().forUpdate().show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.smart.isNull().forReadOnly().show();
        assertSimilar("SELECT T0.smart IS NULL AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testExists() throws Exception {
        final String sql = person2.id.where(person.smart.isNull().exists()).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE EXISTS(SELECT T0.smart IS NULL FROM person AS T0)", sql);
    }


    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.alive.isNull().show();
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

        final List<Boolean> list = person.alive.isNull().list(datasource);
        assertEquals(1, list.size());
        assertEquals(Boolean.TRUE, list.get(0));
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.alive.isNull().show();
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

        person.alive.isNull().scroll(datasource, new Callback<Boolean, SQLException>() {
            int callCount = 0;

            @Override
            public void iterate(final Boolean aBoolean) throws SQLException, BreakException {
                if (callCount++ != 0) {
                    fail("One call expected, actually " + callCount);
                }
                assertEquals(Boolean.TRUE, aBoolean);
            }
        });
        verify(datasource, connection,  statement, resultSet);
    }



    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<Long> alive = new LongColumn("alive", this);
        public Column<Long> smart = new LongColumn("smart", this);
        public Column<Long> cute = new LongColumn("cute", this);
        public Column<Long> speaksJapan = new LongColumn("speaks_japan", this);
        public Column<Long> friendly = new LongColumn("friendly", this);
    }

    private static Person person = new Person();
    private static Person person2 = new Person();


}
