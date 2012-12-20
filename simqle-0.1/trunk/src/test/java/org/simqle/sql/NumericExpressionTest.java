package org.simqle.sql;

import org.simqle.Callback;
import org.simqle.Function;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class NumericExpressionTest extends SqlTestCase {

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.id.plus(two).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.id.plus(two).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? IS NOT NULL", sql);
    }


    public void testSelect() throws Exception {
        final String sql = person.id.plus(two).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.id.plus(two).all().show();
        assertSimilar("SELECT ALL T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.id.plus(two).distinct().show();
        assertSimilar("SELECT DISTINCT T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testSelectForUpdate() throws Exception {
        final String sql = person.id.plus(two).forUpdate().show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testSelectForReadOnly() throws Exception {
        final String sql = person.id.plus(two).forReadOnly().show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.plus(two).where(person.id.eq(two)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 WHERE T0.id = ?", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.id.plus(two).eq(person.id.plus(0))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? = T0.id + ?", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.id.plus(two).ne(person.id.plus(0))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? <> T0.id + ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.id.plus(two).gt(person.id.plus(0))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? > T0.id + ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.id.plus(two).ge(person.id.plus(0))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? >= T0.id + ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.id.plus(two).lt(person.id.plus(0))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? < T0.id + ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.id.plus(two).le(person.id.plus(0))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? <= T0.id + ?", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.id.plus(two).except(person2.id.plus(0)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 EXCEPT SELECT T1.id + ? AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.plus(two).exceptAll(person2.id.plus(0)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id + ? AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.id.plus(two).exceptDistinct(person2.id.plus(0)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id + ? AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.id.plus(two).union(person2.id.plus(0)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 UNION SELECT T1.id + ? AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.plus(two).unionAll(person2.id.plus(0)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 UNION ALL SELECT T1.id + ? AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.id.plus(two).unionDistinct(person2.id.plus(0)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id + ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.id.plus(two).intersect(person2.id.plus(0)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 INTERSECT SELECT T1.id + ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.plus(two).intersectAll(person2.id.plus(0)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id + ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.plus(two).intersectDistinct(person2.id.plus(0)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id + ? AS C0 FROM person AS T1", sql);
    }


    public void testExists() throws Exception {
        String sql = person.id.where(person2.id.plus(two).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1)", sql);

    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.id.plus(two).in(person2.id.plus(two))).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id + ? IN(SELECT T2.id + ? FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.id.plus(two).notIn(person2.id.plus(0))).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id + ? NOT IN(SELECT T2.id + ? FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.id.plus(two).in(person.id.plus(two), person.id.plus(0))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? IN(T0.id + ?, T0.id + ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.id.plus(two).notIn(person.id.plus(two), person.id.plus(0))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? NOT IN(T0.id + ?, T0.id + ?)", sql);
   }

    public void testOrderBy() throws Exception {
        String sql = person.id.plus(two).orderBy(person.id.plus(two)).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0 ORDER BY T0.id + ?", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.id.plus(two).nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id + ? NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.id.plus(two).nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id + ? NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.id.plus(two).desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id + ? DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.id.plus(two).asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id + ? ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = person.id.plus(two).opposite().show();
        assertSimilar("SELECT -(T0.id + ?) AS C0 FROM person AS T0", sql);
    }


    public void testPlus() throws Exception {
        String sql = person.id.plus(two).plus(person.id.plus(two)).show();
        assertSimilar("SELECT T0.id + ? +(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.id.plus(two).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.id + ?)", sql);
    }

    public void testMinus() throws Exception {
        String sql = person.id.plus(two).minus(person.id.plus(two)).show();
        assertSimilar("SELECT T0.id + ? -(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.id.plus(two).mult(person.id.plus(two)).show();
        assertSimilar("SELECT(T0.id + ?) *(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = person.id.plus(two).div(person.id.plus(two)).show();
        assertSimilar("SELECT(T0.id + ?) /(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.id.plus(two).concat(person.id.plus(two)).show();
        assertSimilar("SELECT(T0.id + ?) ||(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.id.plus(two).pair(person.id.plus(two)).show();
        assertSimilar("SELECT T1.id + ? AS C1, T1.id + ? AS C2 FROM person AS T1", sql);
    }

    public void testConvert() throws Exception {
        assertSimilar("SELECT T1.id + ? AS C1 FROM person AS T1", person.id.plus(two).convert(new Function<Number, String>() {
            @Override
            public String apply(final Number arg) {
                return String.valueOf(arg);
            }
        }).show());

    }

    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.id.plus(two).show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setLong(1, 2L);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getBigDecimal(matches("C[0-9]"))).andReturn(new BigDecimal(123));
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        final List<Number> list = person.id.plus(two).list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.id.plus(two).show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setLong(1, 2L);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getBigDecimal(matches("C[0-9]"))).andReturn(new BigDecimal(123));
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        person.id.plus(two).scroll(datasource, new Callback<Number, SQLException>() {
            int callCount = 0;

            @Override
            public void iterate(final Number aNumber) throws SQLException, BreakException {
                if (callCount++ != 0) {
                    fail("One call expected, actually " + callCount);
                }
                assertEquals(123L, aNumber.longValue());
            }
        });
        verify(datasource, connection,  statement, resultSet);
    }



    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
    }
    
    private static DynamicParameter<Long> two = new LongParameter(2L);

    private static Person person = new Person();
    private static Person person2 = new Person();


}
