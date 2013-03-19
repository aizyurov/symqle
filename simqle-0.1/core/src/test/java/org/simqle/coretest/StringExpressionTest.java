package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.TableOrView;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

    public void testWhere() throws Exception {
        final String sql = numberSign.concat(person.id).where(person.id.concat(numberSign).eq(numberSign)).show();
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 WHERE T0.id || ? = ?", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).eq(numberSign)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id = ?", sql);
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

    public void testEqValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).eq("#12")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).ne("#12")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).gt("#12")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).ge("#12")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).lt("#12")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).le("#12")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <= ?", sql);
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


    public void testBooleanValue() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(? || T0.id)", sql);
    }

    public void testPair() throws Exception {
        String sql = numberSign.concat(person.id).pair(person.name).show();
        assertSimilar("SELECT ? || T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testPlus() throws Exception {
        String sql = numberSign.concat(person.id).plus(numberSign.concat(person.id)).show();
        assertSimilar("SELECT(? || T0.id) +(? || T0.id) AS C0 FROM person AS T0", sql);
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

    public void testPlusNumber() throws Exception {
        String sql = numberSign.concat(person.id).plus(2).show();
        assertSimilar("SELECT(? || T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        String sql = numberSign.concat(person.id).minus(2).show();
        assertSimilar("SELECT(? || T0.id) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = numberSign.concat(person.id).mult(2).show();
        assertSimilar("SELECT(? || T0.id) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = numberSign.concat(person.id).div(2).show();
        assertSimilar("SELECT(? || T0.id) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = numberSign.concat(person.id).concat(" test").show();
        assertSimilar("SELECT ? || T0.id || ? AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.id.concat(" test").union(person2.id.concat(" test2")).show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 UNION SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.concat(" test").unionAll(person2.id.concat(" test2")).show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 UNION ALL SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.id.concat(" test").unionDistinct(person2.id.concat(" test2")).show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.id.concat(" test").except(person2.id.concat(" test2")).show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 EXCEPT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.concat(" test").exceptAll(person2.id.concat(" test2")).show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.id.concat(" test").exceptDistinct(person2.id.concat(" test2")).show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.id.concat(" test").intersect(person2.id.concat(" test2")).show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 INTERSECT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.concat(" test").intersectAll(person2.id.concat(" test2")).show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.concat(" test").intersectDistinct(person2.id.concat(" test2")).show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = person.id.where(person2.id.concat(" test").exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id || ? FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.concat(" test").forUpdate().show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.concat(" test").forReadOnly().show();
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testQueryValue() {
        final String sql = person.id.concat(" test").queryValue().orderBy(person2.id).show();
        assertSimilar("SELECT(SELECT T0.id || ? FROM person AS T0) AS C0 FROM person AS T1 ORDER BY T1.id", sql);
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(person.name.concat(" test")).show();
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name || ? END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.name).orElse(person.name.concat(" test")).show();
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name ELSE T0.name || ? END AS C0 FROM person AS T0", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").like(person.name.concat(" tes_"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? LIKE T0.name || ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").notLike(person.name.concat(" tes_"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? NOT LIKE T0.name || ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").like("J%tes_")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").notLike("J%tes_")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? NOT LIKE ?", sql);
    }
    
    public void testCount() throws Exception {
        final String sql = person.name.concat("test").count().show();
        assertSimilar("SELECT COUNT(T1.name || ?) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.name.concat("test").countDistinct().show();
        assertSimilar("SELECT COUNT(DISTINCT T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testSum() throws Exception {
        final String sql = person.name.concat("test").sum().show();
        assertSimilar("SELECT SUM(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testAvg() throws Exception {
        final String sql = person.name.concat("test").avg().show();
        assertSimilar("SELECT AVG(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testMin() throws Exception {
        final String sql = person.name.concat("test").min().show();
        assertSimilar("SELECT MIN(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testMax() throws Exception {
        final String sql = person.name.concat("test").max().show();
        assertSimilar("SELECT MAX(T1.name || ?) AS C1 FROM person AS T1", sql);
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
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        numberSign.concat(person.id).scroll(datasource, new Callback<String>() {
            int callCount = 0;

            @Override
            public boolean iterate(final String aString) {
                if (callCount++ != 0) {
                    fail("One call expected, actually " + callCount);
                }
                assertEquals("#123", aString);
                return true;
            }
        });
        verify(datasource, connection,  statement, resultSet);
    }



    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }
    
    private static DynamicParameter<String> numberSign = DynamicParameter.create(Mappers.STRING, "#");

    private static Person person = new Person();
    private static Person person2 = new Person();


}
