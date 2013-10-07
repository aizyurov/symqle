package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.Mappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractComparisonPredicate;
import org.symqle.sql.AbstractStringExpression;
import org.symqle.sql.Column;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class StringExpressionTest extends SqlTestCase {

    public void testIsNull() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id IS NOT NULL", sql);
    }


    public void testShow() throws Exception {
        final String sql = numberSign.concat(person.id).show(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0", sql);
        assertSimilar(sql, numberSign.concat(person.id).show(new GenericDialect()));
    }

    public void testMap() throws Exception {
        final String sql = numberSign.concat(person.id).map(Mappers.STRING).show(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = numberSign.concat(person.id).selectAll().show(new GenericDialect());
        assertSimilar("SELECT ALL ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = numberSign.concat(person.id).distinct().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = numberSign.concat(person.id).where(person.id.concat(numberSign).eq(numberSign)).show(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 WHERE T0.id || ? = ?", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).eq(numberSign)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id = ?", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).ne(numberSign)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <> ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).gt(numberSign)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id > ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).ge(numberSign)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id >= ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).lt(numberSign)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id < ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).le(numberSign)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <= ?", sql);
    }

    public void testEqValue() throws Exception {
        final AbstractStringExpression<String> stringExpression = numberSign.concat(person.id);
        final AbstractComparisonPredicate predicate = stringExpression.eq("#12");
        final String sql = person.id.where(predicate).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).ne("#12")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).gt("#12")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).ge("#12")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).lt("#12")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(numberSign.concat(person.id).le("#12")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id <= ?", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).in(numberSign.concat(person2.id))).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE ? || T1.id IN(SELECT ? || T2.id FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).notIn(numberSign.concat(person2.id))).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE ? || T1.id NOT IN(SELECT ? || T2.id FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).in("#123", "#124")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).notIn("#123", "#124")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? || T0.id NOT IN(?, ?)", sql);
   }

    public void testOrderBy() throws Exception {
        String sql = numberSign.concat(person.id).orderBy(numberSign.concat(person.id)).show(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id", sql);
    }

    public void testOrderAsc() throws Exception {
        String sql = numberSign.concat(person.id).orderAsc().show(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 ORDER BY C0 ASC", sql);
    }

    public void testOrderDesc() throws Exception {
        String sql = numberSign.concat(person.id).orderDesc().show(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0 ORDER BY C0 DESC", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(numberSign.concat(person.id).nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(numberSign.concat(person.id).nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(numberSign.concat(person.id).desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(numberSign.concat(person.id).asc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY ? || T0.id ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = numberSign.concat(person.id).opposite().show(new GenericDialect());
        assertSimilar("SELECT -(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = numberSign.concat(person.id).cast("CHAR(10)").show(new GenericDialect());
        assertSimilar("SELECT CAST(? || T0.id AS CHAR(10)) AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(numberSign.concat(person.id).asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(? || T0.id)", sql);
    }

    public void testPair() throws Exception {
        String sql = numberSign.concat(person.id).pair(person.name).show(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        String sql = numberSign.concat(person.id).add(numberSign.concat(person.id)).show(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) +(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        String sql = numberSign.concat(person.id).sub(numberSign.concat(person.id)).show(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) -(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = numberSign.concat(person.id).mult(numberSign.concat(person.id)).show(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) *(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = numberSign.concat(person.id).div(numberSign.concat(person.id)).show(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) /(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = numberSign.concat(person.id).concat(numberSign.concat(person.id)).show(new GenericDialect());
        assertSimilar("SELECT ? || T0.id ||(? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        String sql = numberSign.concat(person.id).substring(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? || T0.id FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        String sql = numberSign.concat(person.id).substring(person.id, person.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? || T0.id FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        String sql = numberSign.concat(person.id).substring(2).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? || T0.id FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        String sql = numberSign.concat(person.id).substring(2, 5).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? || T0.id FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        String sql = numberSign.concat(person.id).positionOf(person.id).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.id IN ? || T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        String sql = numberSign.concat(person.id).positionOf("A").show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN ? || T0.id) AS C0 FROM person AS T0", sql);
    }


    public void testAddNumber() throws Exception {
        String sql = numberSign.concat(person.id).add(2).show(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) + ? AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        String sql = numberSign.concat(person.id).sub(2).show(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = numberSign.concat(person.id).mult(2).show(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = numberSign.concat(person.id).div(2).show(new GenericDialect());
        assertSimilar("SELECT(? || T0.id) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = numberSign.concat(person.id).concat(" test").show(new GenericDialect());
        assertSimilar("SELECT ? || T0.id || ? AS C0 FROM person AS T0", sql);
    }

    public void testCollate() throws Exception {
        String sql = numberSign.concat(person.id).collate("latin1_general_ci").show(new GenericDialect());
        // concat have less precedence, so parenthesized
        assertSimilar("SELECT(? || T0.id) COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.id.concat(" test").union(person2.id.concat(" test2")).show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 UNION SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.concat(" test").unionAll(person2.id.concat(" test2")).show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 UNION ALL SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.id.concat(" test").unionDistinct(person2.id.concat(" test2")).show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.id.concat(" test").except(person2.id.concat(" test2")).show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 EXCEPT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.concat(" test").exceptAll(person2.id.concat(" test2")).show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.id.concat(" test").exceptDistinct(person2.id.concat(" test2")).show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = person.id.concat(" test").intersect(person2.id.concat(" test2")).show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 INTERSECT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.concat(" test").intersectAll(person2.id.concat(" test2")).show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.concat(" test").intersectDistinct(person2.id.concat(" test2")).show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id || ? AS C0 FROM person AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = person.id.where(person2.id.concat(" test").exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id || ? FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = person.id.where(person2.id.concat(" test").contains("my test")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.id || ? FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.concat(" test").forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.concat(" test").forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T0.id || ? AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testQueryValue() {
        final String sql = person.id.concat(" test").queryValue().orderBy(person2.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id || ? FROM person AS T0) AS C0 FROM person AS T1 ORDER BY T1.id", sql);
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(person.name.concat(" test")).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name || ? END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.name).orElse(person.name.concat(" test")).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name ELSE T0.name || ? END AS C0 FROM person AS T0", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").like(person.name.concat(" tes_"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? LIKE T0.name || ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").notLike(person.name.concat(" tes_"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? NOT LIKE T0.name || ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").like("J%tes_")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.name.concat(" test").notLike("J%tes_")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name || ? NOT LIKE ?", sql);
    }
    
    public void testCount() throws Exception {
        final String sql = person.name.concat("test").count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.name || ?) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.name.concat("test").countDistinct().show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testSum() throws Exception {
        final String sql = person.name.concat("test").sum().show(new GenericDialect());
        assertSimilar("SELECT SUM(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testAvg() throws Exception {
        final String sql = person.name.concat("test").avg().show(new GenericDialect());
        assertSimilar("SELECT AVG(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testMin() throws Exception {
        final String sql = person.name.concat("test").min().show(new GenericDialect());
        assertSimilar("SELECT MIN(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testMax() throws Exception {
        final String sql = person.name.concat("test").max().show(new GenericDialect());
        assertSimilar("SELECT MAX(T1.name || ?) AS C1 FROM person AS T1", sql);
    }
    
    



    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DatabaseGate gate, final AbstractStringExpression<String> stringExpression) throws SQLException {
                final List<String> list = stringExpression.list(gate);
                assertEquals(1, list.size());
                assertEquals("#123", list.get(0));
            }
        }.play();

    }


    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DatabaseGate gate, final AbstractStringExpression<String> stringExpression) throws SQLException {
                stringExpression.scroll(gate, new Callback<String>() {
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
            }
        }.play();

    }

    private static abstract class Scenario {
        public void play() throws Exception {
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            final AbstractStringExpression<String> stringExpression = numberSign.concat(person.id);
            final String queryString = stringExpression.show(new GenericDialect());
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(new GenericDialect());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            statement.setString(1, "#");
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getString(matches("C[0-9]"))).andReturn("#123");
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection, statement, resultSet);

            runQuery(gate, stringExpression);
            verify(gate, connection, statement, resultSet);
        }

        protected abstract void runQuery(final DatabaseGate gate, final AbstractStringExpression<String> stringExpression) throws SQLException;
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
