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
public class BooleanTestTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().and(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS TRUE AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().or(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS TRUE OR T0.smart", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().pair(person.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0, T0.smart AS C1 FROM person AS T0", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive IS TRUE", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT NULL", sql);
    }


    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).and(person.friendly.booleanValue().isTrue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND T0.friendly IS TRUE", sql);
    }

    public void testOrNegate() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).negate().isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT(T0.alive OR T0.smart)) IS TRUE", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).or(person.friendly.booleanValue().isTrue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly IS TRUE", sql);
    }

    public void testSelect() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().all().show();
        assertSimilar("SELECT ALL T0.alive IS TRUE AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().distinct().show();
        assertSimilar("SELECT DISTINCT T0.alive IS TRUE AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().where(person.smart.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 WHERE T0.smart", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().eq(person.smart.booleanValue().isTrue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) =(T0.smart IS TRUE)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().ne(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().gt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().ge(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().lt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) <(T0.smart)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue().le(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) <=(T0.smart)", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().isTrue().in(person2.alive.booleanValue().isTrue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart IS TRUE) IN(SELECT T2.alive IS TRUE FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().isTrue().notIn(person2.alive.booleanValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart IS TRUE) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().isTrue().in(person.alive.booleanValue().isTrue(), person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart IS TRUE) IN(T0.alive IS TRUE, T0.friendly)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().isTrue().notIn(person.alive.booleanValue(), person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart IS TRUE) NOT IN(T0.alive, T0.friendly)", sql);
   }

    public void testOpposite() throws Exception {
        final String sql = person.smart.booleanValue().isTrue().opposite().show();
        assertSimilar("SELECT -(T0.smart IS TRUE) AS C0 FROM person AS T0", sql);
    }

    public void testPlus() throws Exception {
        String sql = person.smart.booleanValue().isTrue().plus(person.alive.booleanValue().isTrue()).show();
        assertSimilar("SELECT(T0.smart IS TRUE) +(T0.alive IS TRUE) AS C0 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        String sql = person.smart.booleanValue().isTrue().plus(2).show();
        assertSimilar("SELECT(T0.smart IS TRUE) + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.smart.booleanValue().isTrue().booleanValue().show();
        assertSimilar("SELECT(T0.smart IS TRUE) AS C0 FROM person AS T0", sql);
    }

    public void testMinus() throws Exception {
        String sql = person.smart.booleanValue().isTrue().minus(person.alive.booleanValue()).show();
        assertSimilar("SELECT(T0.smart IS TRUE) -(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        String sql = person.smart.booleanValue().isTrue().minus(2).show();
        assertSimilar("SELECT(T0.smart IS TRUE) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.smart.booleanValue().isTrue().mult(person.alive.booleanValue().isTrue()).show();
        assertSimilar("SELECT(T0.smart IS TRUE) *(T0.alive IS TRUE) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.smart.booleanValue().isTrue().mult(2).show();
        assertSimilar("SELECT(T0.smart IS TRUE) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = person.smart.booleanValue().isTrue().div(person.alive.booleanValue()).show();
        assertSimilar("SELECT(T0.smart IS TRUE) /(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.smart.booleanValue().isTrue().div(2).show();
        assertSimilar("SELECT(T0.smart IS TRUE) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.smart.booleanValue().isTrue().concat(person.alive.booleanValue()).show();
        assertSimilar("SELECT(T0.smart IS TRUE) ||(T0.alive) AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = person.smart.booleanValue().isTrue().concat(" test").show();
        assertSimilar("SELECT(T0.smart IS TRUE) || ? AS C0 FROM person AS T0", sql);
    }
    
    public void testOrderBy() throws Exception {
        String sql = person.alive.booleanValue().isTrue().orderBy(person.smart.booleanValue().isTrue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 ORDER BY T0.smart IS TRUE", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().isTrue().nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart IS TRUE NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().isTrue().nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart IS TRUE NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().isTrue().desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart IS TRUE DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.smart.booleanValue().isTrue().asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.smart IS TRUE ASC", sql);
    }
    

    public void testList() throws Exception {
        final AbstractBooleanTest[] tests = {
                person.alive.booleanValue().isTrue(),
                person.alive.booleanValue().isNotTrue(),
                person.alive.booleanValue().isFalse(),
                person.alive.booleanValue().isNotFalse(),
                person.alive.booleanValue().isUnknown(),
                person.alive.booleanValue().isNotUnknown()
        };
        for (final AbstractBooleanTest booleanTest : tests) {
            final String queryString = booleanTest.show();
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

            final List<Boolean> list = booleanTest.list(datasource);
            assertEquals(1, list.size());
            assertEquals(Boolean.TRUE, list.get(0));
            verify(datasource, connection, statement, resultSet);
        }
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.alive.booleanValue().isTrue().show();
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

        person.alive.booleanValue().isTrue().scroll(datasource, new Callback<Boolean, SQLException>() {
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
    
    public void testUnion() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().union(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 UNION SELECT T1.alive AS C0 FROM person AS T1", sql);
    }
    
    public void testUnionAll() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().unionAll(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 UNION ALL SELECT T1.alive AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().unionDistinct(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.alive AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().except(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 EXCEPT SELECT T1.alive AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().exceptAll(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.alive AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().exceptDistinct(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.alive AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().intersect(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 INTERSECT SELECT T1.alive AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().intersectAll(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.alive AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().intersectDistinct(person2.alive.booleanValue()).show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.alive AS C0 FROM person AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = person2.id.where(person.alive.booleanValue().isTrue().exists()).show();
        assertSimilar("SELECT T1.id AS C0 FROM person AS T1 WHERE EXISTS(SELECT T0.alive IS TRUE FROM person AS T0)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().forUpdate().show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().forReadOnly().show();
        assertSimilar("SELECT T0.alive IS TRUE AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testSubquery() throws Exception {
        final String sql = person.alive.booleanValue().isTrue().queryValue().where(person2.alive.booleanValue()).show();
        assertSimilar("SELECT(SELECT T0.alive IS TRUE FROM person AS T0) AS C1 FROM person AS T1 WHERE T1.alive", sql);
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
