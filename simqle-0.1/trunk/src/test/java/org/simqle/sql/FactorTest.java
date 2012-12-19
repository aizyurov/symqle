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
public class FactorTest extends SqlTestCase {

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.id.opposite().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.id.opposite().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id IS NOT NULL", sql);
    }


    public void testSelect() throws Exception {
        final String sql = person.id.opposite().show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.id.opposite().all().show();
        assertSimilar("SELECT ALL - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.id.opposite().distinct().show();
        assertSimilar("SELECT DISTINCT - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectForUpdate() throws Exception {
        final String sql = person.id.opposite().forUpdate().show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testSelectForReadOnly() throws Exception {
        final String sql = person.id.opposite().forReadOnly().show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.opposite().where(person.smart.booleanValue()).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 WHERE T0.smart", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.id.opposite().eq(person.id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id = T0.id", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.id.opposite().ne(person.id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id <> T0.id", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.id.opposite().gt(person.id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id > T0.id", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.id.opposite().ge(person.id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id >= T0.id", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.id.opposite().lt(person.id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id < T0.id", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.id.opposite().le(person.id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id <= T0.id", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.id.opposite().except(person2.id).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 EXCEPT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.opposite().exceptAll(person2.id).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.id.opposite().exceptDistinct(person2.id).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.id.opposite().union(person2.id).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 UNION SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.opposite().unionAll(person2.id).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 UNION ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.id.opposite().unionDistinct(person2.id).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.id.opposite().intersect(person2.id).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 INTERSECT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.opposite().intersectAll(person2.id).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.opposite().intersectDistinct(person2.id).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }


    public void testExists() throws Exception {
        String sql = person.id.where(person2.id.opposite().exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT - T1.id FROM person AS T1)", sql);

    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.id.opposite().in(person2.id.opposite())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE - T1.id IN(SELECT - T2.id FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.id.opposite().notIn(person2.id)).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE - T1.id NOT IN(SELECT T2.id FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.id.opposite().in(person.id.opposite(), person.id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id IN(- T0.id, T0.id)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.id.opposite().notIn(person.id.opposite(), person.id)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE - T0.id NOT IN(- T0.id, T0.id)", sql);
   }

    public void testOrderBy() throws Exception {
        String sql = person.id.opposite().orderBy(person.id.opposite()).show();
        assertSimilar("SELECT - T0.id AS C0 FROM person AS T0 ORDER BY - T0.id", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.id.opposite().nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY - T0.id NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.id.opposite().nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY - T0.id NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.id.opposite().desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY - T0.id DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.id.opposite().asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY - T0.id ASC", sql);
    }


    public void testNumericValue() throws Exception {
        final String sql = person.id.where(person.id.opposite().numericValue().eq(person.id.opposite().numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(- T0.id) =(- T0.id)", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = person.id.opposite().opposite().show();
        assertSimilar("SELECT -(- T0.id) AS C0 FROM person AS T0", sql);
    }


    public void testPlus() throws Exception {
        String sql = person.id.opposite().plus(person.id.opposite()).show();
        assertSimilar("SELECT - T0.id + - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.id.opposite().booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(- T0.id)", sql);
    }

    public void testMinus() throws Exception {
        String sql = person.id.opposite().minus(person.id.opposite()).show();
        assertSimilar("SELECT - T0.id - - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.id.opposite().mult(person.id.opposite()).show();
        assertSimilar("SELECT - T0.id * - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = person.id.opposite().div(person.id.opposite()).show();
        assertSimilar("SELECT - T0.id / - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.id.opposite().concat(person.id.opposite()).show();
        assertSimilar("SELECT(- T0.id) ||(- T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.id.opposite().pair(person.id.opposite()).show();
        assertSimilar("SELECT - T1.id AS C1, - T1.id AS C2 FROM person AS T1", sql);
    }

    public void testConvert() throws Exception {
        assertSimilar("SELECT - T1.id AS C1 FROM person AS T1", person.id.opposite().convert(new Function<Long, String>() {
            @Override
            public String apply(final Long arg) {
                return String.valueOf(arg);
            }
        }).show());

    }

    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.id.opposite().show();
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

        final List<Long> list = person.id.opposite().list(datasource);
        assertEquals(1, list.size());
        assertEquals(Long.valueOf(123), list.get(0));
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.id.opposite().show();
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

        person.id.opposite().scroll(datasource, new Callback<Long, SQLException>() {
            int callCount = 0;

            @Override
            public void iterate(final Long aLong) throws SQLException, BreakException {
                if (callCount++ != 0) {
                    fail("One call expected, actually " + callCount);
                }
                assertEquals(Long.valueOf(123), aLong);
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
