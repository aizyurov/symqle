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
public class BooleanTermTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).and(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive AND T0.cute AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).or(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive AND T0.cute OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(T0.alive AND T0.cute)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT NULL", sql);
    }


    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).and(person.friendly.booleanValue().and(person.cute.booleanValue()))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND(T0.friendly AND T0.cute)", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).or(person.friendly.booleanValue().and(person.cute.booleanValue()))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly AND T0.cute", sql);
    }

    public void testSelect() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).all().show();
        assertSimilar("SELECT ALL T0.alive AND T0.cute AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).distinct().show();
        assertSimilar("SELECT DISTINCT T0.alive AND T0.cute AS C0 FROM person AS T0", sql);
    }

    public void testSelectForUpdate() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).forUpdate().show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testSelectForReadOnly() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).forReadOnly().show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).where(person.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 WHERE T0.smart", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).eq(person.smart.booleanValue().and(person.cute.booleanValue()))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) =(T0.smart AND T0.cute)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).ne(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).gt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).ge(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).lt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) <(T0.smart)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).le(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) <=(T0.smart)", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).except(person2.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 EXCEPT SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).exceptAll(person2.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).exceptDistinct(person2.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).union(person2.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 UNION SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).unionAll(person2.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 UNION ALL SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).unionDistinct(person2.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).intersect(person2.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 INTERSECT SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).intersectAll(person2.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.smart AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.alive.booleanValue().and(person.cute.booleanValue()).intersectDistinct(person2.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.smart AS C0 FROM person AS T1", sql);
    }


    public void testExists() throws Exception {
        String sql = person.id.where(person2.smart.booleanValue().and(person2.cute.booleanValue()).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.smart AND T1.cute FROM person AS T1)", sql);

    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().and(person.cute.booleanValue()).in(person2.alive.booleanValue().and(person2.cute.booleanValue()))).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart AND T1.cute) IN(SELECT T2.alive AND T2.cute FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().and(person.cute.booleanValue()).notIn(person2.alive.booleanValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart AND T1.cute) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().and(person.cute.booleanValue()).in(person.alive.booleanValue().and(person.cute.booleanValue()), person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart AND T0.cute) IN(T0.alive AND T0.cute, T0.friendly)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().and(person.cute.booleanValue()).notIn(person.alive.booleanValue(), person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart AND T0.cute) NOT IN(T0.alive, T0.friendly)", sql);
   }

    public void testOrderBy() throws Exception {
        String sql = person.alive.booleanValue().and(person.cute.booleanValue()).orderBy(person.smart.booleanValue().and(person.cute.booleanValue())).show();
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0 ORDER BY T0.smart AND T0.cute", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().and(person.cute.booleanValue()).nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart AND T0.cute NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().and(person.cute.booleanValue()).nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart AND T0.cute NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().and(person.cute.booleanValue()).desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart AND T0.cute DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().and(person.cute.booleanValue()).asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart AND T0.cute ASC", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = person.smart.booleanValue().and(person.cute.booleanValue()).opposite().show();
        assertSimilar("SELECT -(T0.smart AND T0.cute) AS C0 FROM person AS T0", sql);
    }


    public void testPlus() throws Exception {
        String sql = person.smart.booleanValue().and(person.cute.booleanValue()).plus(person.alive.booleanValue().and(person.cute.booleanValue())).show();
        assertSimilar("SELECT(T0.smart AND T0.cute) +(T0.alive AND T0.cute) AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.smart.booleanValue().and(person.cute.booleanValue()).booleanValue().show();
        assertSimilar("SELECT(T0.smart AND T0.cute) AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        String sql = person.smart.booleanValue().and(person.cute.booleanValue()).minus(person.alive.booleanValue()).show();
        assertSimilar("SELECT(T0.smart AND T0.cute) -(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.smart.booleanValue().and(person.cute.booleanValue()).mult(person.alive.booleanValue().and(person.cute.booleanValue())).show();
        assertSimilar("SELECT(T0.smart AND T0.cute) *(T0.alive AND T0.cute) AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = person.smart.booleanValue().and(person.cute.booleanValue()).div(person.alive.booleanValue()).show();
        assertSimilar("SELECT(T0.smart AND T0.cute) /(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.smart.booleanValue().and(person.cute.booleanValue()).concat(person.alive.booleanValue()).show();
        assertSimilar("SELECT(T0.smart AND T0.cute) ||(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.smart.booleanValue().and(person.cute.booleanValue()).pair(person.alive.booleanValue()).show();
        assertSimilar("SELECT T1.smart AND T1.cute AS C1, T1.alive AS C2 FROM person AS T1", sql);
    }

    public void testConvert() throws Exception {
        assertSimilar("SELECT T1.alive AND T1.cute AS C1 FROM person AS T1", person.alive.booleanValue().and(person.cute.booleanValue()).convert(new Function<Boolean, String>() {
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
        final String queryString = person.alive.booleanValue().and(person.cute.booleanValue()).show();
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

        final List<Boolean> list = person.alive.booleanValue().and(person.cute.booleanValue()).list(datasource);
        assertEquals(1, list.size());
        assertEquals(Boolean.TRUE, list.get(0));
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.alive.booleanValue().and(person.cute.booleanValue()).show();
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

        person.alive.booleanValue().and(person.cute.booleanValue()).scroll(datasource, new Callback<Boolean, SQLException>() {
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
