package org.simqle.sql;

import org.simqle.Callback;
import org.simqle.Element;
import org.simqle.Function;
import org.simqle.SqlParameter;

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
public class StringExpressionTest extends SqlTestCase {

    public void testIsNull() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id IS NOT NULL", sql);
    }


    public void testSelect() throws Exception {
        final String sql = numberSign.concat(person.id).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = numberSign.concat(person.id).all().show();
        assertSimilar("SELECT ALL ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = numberSign.concat(person.id).distinct().show();
        assertSimilar("SELECT DISTINCT ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectForUpdate() throws Exception {
        final String sql = numberSign.concat(person.id).forUpdate().show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testSelectForReadOnly() throws Exception {
        final String sql = numberSign.concat(person.id).forReadOnly().show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testWhere() throws Exception {
        final String sql = numberSign.concat(person.id).where(person.id.concat(numberSign).eq(numberSign)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 WHERE T0.id || ? = ?", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).eq(numberSign)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id = ?", sql);
    }

    public void testNumericValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).numericValue().eq(person.id.numericValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(? || T0.id) = T0.id", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).ne(numberSign)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <> ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).gt(numberSign)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id > ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).ge(numberSign)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id >= ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).lt(numberSign)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id < ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).le(numberSign)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <= ?", sql);
    }

    public void testExcept() throws Exception {
        final String sql = numberSign.concat(person.id).except(numberSign.concat(person2.id)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 EXCEPT SELECT ? || T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = numberSign.concat(person.id).exceptAll(numberSign.concat(person2.id)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 EXCEPT ALL SELECT ? || T1.id AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = numberSign.concat(person.id).exceptDistinct(numberSign.concat(person2.id)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT ? || T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final String sql = numberSign.concat(person.id).union(numberSign.concat(person2.id)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 UNION SELECT ? || T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = numberSign.concat(person.id).unionAll(numberSign.concat(person2.id)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 UNION ALL SELECT ? || T1.id AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = numberSign.concat(person.id).unionDistinct(numberSign.concat(person2.id)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 UNION DISTINCT SELECT ? || T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = numberSign.concat(person.id).intersect(numberSign.concat(person2.id)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 INTERSECT SELECT ? || T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = numberSign.concat(person.id).intersectAll(numberSign.concat(person2.id)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 INTERSECT ALL SELECT ? || T1.id AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = numberSign.concat(person.id).intersectDistinct(numberSign.concat(person2.id)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT ? || T1.id AS C0 FROM person AS T1", sql);
    }


    public void testExists() throws Exception {
        String sql = person.id.where(numberSign.concat(person2.id).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT ? || T1.id FROM person AS T1)", sql);

    }

    public void testIn() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).in(numberSign.concat(person2.id))).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE ? || T1.id IN(SELECT ? || T2.id FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).notIn(numberSign.concat(person2.id))).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE ? || T1.id NOT IN(SELECT ? || T2.id FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).in(numberSign.concat(person.id), numberSign.concat(person.id).concat(numberSign))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id IN(? || T0.id, ? || T0.id || ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).notIn(numberSign.concat(person.id), numberSign.concat(person.id).concat(numberSign))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id NOT IN(? || T0.id, ? || T0.id || ?)", sql);
   }

    public void testOrderBy() throws Exception {
        String sql = numberSign.concat(person.id).orderBy(numberSign.concat(person.id)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(numberSign.concat(person.id).nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(numberSign.concat(person.id).nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(numberSign.concat(person.id).desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(numberSign.concat(person.id).asc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = numberSign.concat(person.id).opposite().show();
        assertSimilar("SELECT -(? || T0.id) AS C0 FROM person AS T0", sql);
    }


    public void testPlus() throws Exception {
        String sql = numberSign.concat(person.id).plus(numberSign.concat(person.id)).show();
        assertSimilar("SELECT(? || T0.id) +(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(? || T0.id)", sql);
    }

    public void testMinus() throws Exception {
        String sql = numberSign.concat(person.id).minus(numberSign.concat(person.id)).show();
        assertSimilar("SELECT(? || T0.id) -(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = numberSign.concat(person.id).mult(numberSign.concat(person.id)).show();
        assertSimilar("SELECT(? || T0.id) *(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = numberSign.concat(person.id).div(numberSign.concat(person.id)).show();
        assertSimilar("SELECT(? || T0.id) /(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = numberSign.concat(person.id).concat(numberSign.concat(person.id)).show();
        assertSimilar("SELECT ? || T0.id ||(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final String sql = numberSign.concat(person.id).pair(numberSign.concat(person.id)).show();
        assertSimilar("SELECT ? || T1.id AS C1, ? || T1.id AS C2 FROM person AS T1", sql);
    }

    public void testConvert() throws Exception {
        assertSimilar("SELECT ? || T1.id AS C1 FROM person AS T1", numberSign.concat(person.id).convert(new Function<String, String>() {
            @Override
            public String apply(final String arg) {
                return arg;
            }
        }).show());

    }

    public void testList() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = numberSign.concat(person.id).show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setString(1, "#");
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(matches("C[0-9]"))).andReturn("#123");
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        final List<String> list = numberSign.concat(person.id).list(datasource);
        assertEquals(1, list.size());
        assertEquals("#123", list.get(0));
        verify(datasource, connection, statement, resultSet);
    }


    public void testScroll() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final String queryString = numberSign.concat(person.id).show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setString(1, "#");
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(matches("C[0-9]"))).andReturn("#123");
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        numberSign.concat(person.id).scroll(datasource, new Callback<String, SQLException>() {
            int callCount = 0;

            @Override
            public void iterate(final String aString) throws SQLException, BreakException {
                if (callCount++ != 0) {
                    fail("One call expected, actually " + callCount);
                }
                assertEquals("#123", aString);
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
    
    private static DynamicParameter<String> numberSign = new DynamicParameter<String>() {
        @Override
        protected void set(final SqlParameter p) throws SQLException {
            p.setString("#");
        }

        @Override
        public String value(final Element element) throws SQLException {
            return element.getString();
        }
    };

    private static Person person = new Person();
    private static Person person2 = new Person();


}
