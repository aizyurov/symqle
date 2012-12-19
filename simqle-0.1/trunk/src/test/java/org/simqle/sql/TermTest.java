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
public class TermTest extends SqlTestCase {

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.id.mult(two).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.id.mult(two).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? IS NOT NULL", sql);
    }


    public void testSelect() throws Exception {
        final String sql = person.id.mult(two).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.id.mult(two).all().show();
        assertSimilar("SELECT ALL T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.id.mult(two).distinct().show();
        assertSimilar("SELECT DISTINCT T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testSelectForUpdate() throws Exception {
        final String sql = person.id.mult(two).forUpdate().show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testSelectForReadOnly() throws Exception {
        final String sql = person.id.mult(two).forReadOnly().show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.mult(two).where(person.id.eq(two)).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 WHERE T0.id = ?", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.id.mult(two).eq(person.id.numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? = T0.id", sql);
    }

    public void testNumericValue() throws Exception {
        final String sql = person.id.where(person.id.mult(two).numericValue().eq(person.id.numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.id * ?) = T0.id", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.id.mult(two).ne(person.id.numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? <> T0.id", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.id.mult(two).gt(person.id.numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? > T0.id", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.id.mult(two).ge(person.id.numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? >= T0.id", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.id.mult(two).lt(person.id.numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? < T0.id", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.id.mult(two).le(person.id.numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? <= T0.id", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.id.mult(two).except(person2.id.numericValue()).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 EXCEPT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.mult(two).exceptAll(person2.id.numericValue()).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.id.mult(two).exceptDistinct(person2.id.numericValue()).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.id.mult(two).union(person2.id.numericValue()).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 UNION SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.mult(two).unionAll(person2.id.numericValue()).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 UNION ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.id.mult(two).unionDistinct(person2.id.numericValue()).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.id.mult(two).intersect(person2.id.numericValue()).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 INTERSECT SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.mult(two).intersectAll(person2.id.numericValue()).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.mult(two).intersectDistinct(person2.id.numericValue()).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id AS C0 FROM person AS T1", sql);
    }


    public void testExists() throws Exception {
        String sql = person.id.where(person2.id.mult(two).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1)", sql);

    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.id.mult(two).in(person2.id.mult(two))).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id * ? IN(SELECT T2.id * ? FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.id.mult(two).notIn(person2.id.numericValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id * ? NOT IN(SELECT T2.id FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.id.mult(two).in(person.id.mult(two), person.id.numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? IN(T0.id * ?, T0.id)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.id.mult(two).notIn(person.id.mult(two), person.id.numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? NOT IN(T0.id * ?, T0.id)", sql);
   }

    public void testOrderBy() throws Exception {
        String sql = person.id.mult(two).orderBy(person.id.mult(two)).show();
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 ORDER BY T0.id * ?", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.id.mult(two).nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id * ? NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.id.mult(two).nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id * ? NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.id.mult(two).desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id * ? DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.id.mult(two).asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id * ? ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = person.id.mult(two).opposite().show();
        assertSimilar("SELECT -(T0.id * ?) AS C0 FROM person AS T0", sql);
    }


    public void testPlus() throws Exception {
        String sql = person.id.mult(two).plus(person.id.mult(two)).show();
        assertSimilar("SELECT T0.id * ? + T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        String sql = person.id.mult(two).plus(2).show();
        assertSimilar("SELECT T0.id * ? + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.id.mult(two).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.id * ?)", sql);
    }

    public void testMinus() throws Exception {
        String sql = person.id.mult(two).minus(person.id.mult(two)).show();
        assertSimilar("SELECT T0.id * ? - T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        String sql = person.id.mult(two).minus(2).show();
        assertSimilar("SELECT T0.id * ? - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.id.mult(two).mult(person.id.mult(two)).show();
        assertSimilar("SELECT T0.id * ? *(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.id.mult(two).mult(2).show();
        assertSimilar("SELECT T0.id * ? * ? AS C0 FROM person AS T0", sql);
    }


    public void testDiv() throws Exception {
        String sql = person.id.mult(two).div(person.id.mult(two)).show();
        assertSimilar("SELECT T0.id * ? /(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.id.mult(two).div(2).show();
        assertSimilar("SELECT T0.id * ? / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.id.mult(two).concat(person.id.mult(two)).show();
        assertSimilar("SELECT(T0.id * ?) ||(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = person.id.mult(two).concat(" ids").show();
        assertSimilar("SELECT(T0.id * ?) || ? AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.id.mult(two).pair(person.id.mult(two)).show();
        assertSimilar("SELECT T1.id * ? AS C1, T1.id * ? AS C2 FROM person AS T1", sql);
    }

    public void testConvert() throws Exception {
        assertSimilar("SELECT T1.id * ? AS C1 FROM person AS T1", person.id.mult(two).convert(new Function<Number, String>() {
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
        final String queryString = person.id.mult(two).show();
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

        final List<Number> list = person.id.mult(two).list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = person.id.mult(two).show();
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

        person.id.mult(two).scroll(datasource, new Callback<Number, SQLException>() {
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
