package org.simqle.sql;

import org.simqle.Callback;
import org.simqle.Function;

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
public class BooleanFactorTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().and(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().or(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(NOT T0.alive)", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT NULL", sql);
    }


    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).and(person.friendly.booleanValue().negate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND NOT T0.friendly", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).or(person.friendly.booleanValue().negate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR NOT T0.friendly", sql);
    }

    public void testSelect() throws Exception {
        final String sql = person.alive.booleanValue().negate().show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.alive.booleanValue().negate().all().show();
        assertSimilar("SELECT ALL NOT T0.alive AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.alive.booleanValue().negate().distinct().show();
        assertSimilar("SELECT DISTINCT NOT T0.alive AS C0 FROM person AS T0", sql);
    }

    public void testSelectForUpdate() throws Exception {
        final String sql = person.alive.booleanValue().negate().forUpdate().show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testSelectForReadOnly() throws Exception {
        final String sql = person.alive.booleanValue().negate().forReadOnly().show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.alive.booleanValue().negate().where(person.smart.booleanValue()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 WHERE T0.smart", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().eq(person.smart.booleanValue().negate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) =(NOT T0.smart)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().ne(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().gt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().ge(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().lt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) <(T0.smart)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().le(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) <=(T0.smart)", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.alive.booleanValue().negate().except(person2.smart.booleanValue()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 EXCEPT SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.alive.booleanValue().negate().exceptAll(person2.smart.booleanValue()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.alive.booleanValue().negate().exceptDistinct(person2.smart.booleanValue()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.alive.booleanValue().negate().union(person2.smart.booleanValue()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 UNION SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.alive.booleanValue().negate().unionAll(person2.smart.booleanValue()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 UNION ALL SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.alive.booleanValue().negate().unionDistinct(person2.smart.booleanValue()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.alive.booleanValue().negate().intersect(person2.smart.booleanValue()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 INTERSECT SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.alive.booleanValue().negate().intersectAll(person2.smart.booleanValue()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.alive.booleanValue().negate().intersectDistinct(person2.smart.booleanValue()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.smart AS C0 FROM person AS T1", sql);
    }


    public void testExists() throws Exception {
        String sql = person.id.where(person2.smart.booleanValue().negate().exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT NOT T1.smart FROM person AS T1)", sql);

    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().negate().in(person2.alive.booleanValue().negate())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(NOT T1.smart) IN(SELECT NOT T2.alive FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().negate().notIn(person2.alive.booleanValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(NOT T1.smart) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().negate().in(person.alive.booleanValue().negate(), person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.smart) IN(NOT T0.alive, T0.friendly)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().negate().notIn(person.alive.booleanValue(), person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.smart) NOT IN(T0.alive, T0.friendly)", sql);
   }

    public void testOrderBy() throws Exception {
        String sql = person.alive.booleanValue().negate().orderBy(person.smart.booleanValue().negate()).show();
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0 ORDER BY NOT T0.smart", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().negate().nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY NOT T0.smart NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().negate().nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY NOT T0.smart NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().negate().desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY NOT T0.smart DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().negate().asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY NOT T0.smart ASC", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = person.smart.booleanValue().negate().opposite().show();
        assertSimilar("SELECT -(NOT T0.smart) AS C0 FROM person AS T0", sql);
    }


    public void testPlus() throws Exception {
        String sql = person.smart.booleanValue().negate().plus(person.alive.booleanValue().negate()).show();
        assertSimilar("SELECT(NOT T0.smart) +(NOT T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        String sql = person.smart.booleanValue().negate().plus(2).show();
        assertSimilar("SELECT(NOT T0.smart) + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.smart.booleanValue().negate().booleanValue().show();
        assertSimilar("SELECT(NOT T0.smart) AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        String sql = person.smart.booleanValue().negate().minus(person.alive.booleanValue()).show();
        assertSimilar("SELECT(NOT T0.smart) -(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        String sql = person.smart.booleanValue().negate().minus(2).show();
        assertSimilar("SELECT(NOT T0.smart) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.smart.booleanValue().negate().mult(person.alive.booleanValue().negate()).show();
        assertSimilar("SELECT(NOT T0.smart) *(NOT T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.smart.booleanValue().negate().mult(2).show();
        assertSimilar("SELECT(NOT T0.smart) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = person.smart.booleanValue().negate().div(person.alive.booleanValue()).show();
        assertSimilar("SELECT(NOT T0.smart) /(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.smart.booleanValue().negate().div(2).show();
        assertSimilar("SELECT(NOT T0.smart) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.smart.booleanValue().negate().concat(person.alive.booleanValue()).show();
        assertSimilar("SELECT(NOT T0.smart) ||(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = person.smart.booleanValue().negate().concat(" test").show();
        assertSimilar("SELECT(NOT T0.smart) || ? AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.smart.booleanValue().negate().pair(person.alive.booleanValue()).show();
        assertSimilar("SELECT NOT T1.smart AS C1, T1.alive AS C2 FROM person AS T1", sql);
    }

    public void testConvert() throws Exception {
        assertSimilar("SELECT NOT T1.alive AS C1 FROM person AS T1", person.alive.booleanValue().negate().convert(new Function<Boolean, String>() {
            @Override
            public String apply(final Boolean arg) {
                return String.valueOf(arg);
            }
        }).show());

    }

    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.alive.booleanValue().negate().show();
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

        final List<Boolean> list = person.alive.booleanValue().negate().list(datasource);
        assertEquals(1, list.size());
        assertEquals(Boolean.TRUE, list.get(0));
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.alive.booleanValue().negate().show();
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

        person.alive.booleanValue().negate().scroll(datasource, new Callback<Boolean, SQLException>() {
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
