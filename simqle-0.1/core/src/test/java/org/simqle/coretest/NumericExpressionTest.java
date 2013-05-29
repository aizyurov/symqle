package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractNumericExpression;
import org.simqle.sql.Column;
import org.simqle.sql.DialectDataSource;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.GenericDialect;
import org.simqle.sql.TableOrView;

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


    public void testShow() throws Exception {
        final String sql = person.id.plus(two).show();
        assertSimilar("SELECT T0.id + ? AS C0 FROM person AS T0", sql);
        final String sql2 = person.id.plus(two).show(GenericDialect.get());
        assertSimilar(sql, sql2);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.id.plus(two).all().show();
        assertSimilar("SELECT ALL T0.id + ? AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.id.plus(two).distinct().show();
        assertSimilar("SELECT DISTINCT T0.id + ? AS C0 FROM person AS T0", sql);
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

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.id.plus(two).eq(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.id.plus(two).ne(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.id.plus(two).gt(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.id.plus(two).ge(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.id.plus(two).lt(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.id.plus(two).le(0L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? <= ?", sql);
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
        String sql = person.id.where(person.id.plus(two).in(10L, 12L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.id.plus(two).notIn(10L, 12L)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? NOT IN(?, ?)", sql);
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

    public void testPair() throws Exception {
        String sql = person.id.plus(two).pair(person.name).show();
        assertSimilar("SELECT T0.id + ? AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testPlusNumber() throws Exception {
        String sql = person.id.plus(2).plus(3).show();
        assertSimilar("SELECT T0.id + ? + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.id.plus(two).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.id + ?)", sql);
    }

    public void testMinus() throws Exception {
        String sql = person.id.plus(two).minus(person.id.plus(two)).show();
        assertSimilar("SELECT T0.id + ? -(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testMinusNumber() throws Exception {
        String sql = person.id.plus(two).minus(2).show();
        assertSimilar("SELECT T0.id + ? - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.id.plus(two).mult(person.id.plus(two)).show();
        assertSimilar("SELECT(T0.id + ?) *(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.id.plus(two).mult(2).show();
        assertSimilar("SELECT(T0.id + ?) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        String sql = person.id.plus(two).div(person.id.plus(two)).show();
        assertSimilar("SELECT(T0.id + ?) /(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.id.plus(two).div(2).show();
        assertSimilar("SELECT(T0.id + ?) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.id.plus(two).concat(person.id.plus(two)).show();
        assertSimilar("SELECT(T0.id + ?) ||(T0.id + ?) AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = person.id.plus(two).concat(" id").show();
        assertSimilar("SELECT(T0.id + ?) || ? AS C0 FROM person AS T0", sql);
    }
    
    public void testUnion() throws Exception {
        final String sql = employee.id.where(person.id.plus(2).union(person2.id.plus(1).where(person2.name.eq(employee.name))).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 UNION SELECT T2.id + ? FROM person AS T2 WHERE T2.name = T0.name)", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = employee.id.where(person.id.plus(2).unionAll(person2.id.plus(1).where(person2.name.eq(employee.name))).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 UNION ALL SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = employee.id.where(person.id.plus(2).unionDistinct(person2.id.plus(1).where(person2.name.eq(employee.name))).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 UNION DISTINCT SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExcept() throws Exception {
        final String sql = employee.id.where(person.id.plus(2).except(person2.id.plus(1).where(person2.name.eq(employee.name))).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 EXCEPT SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = employee.id.where(person.id.plus(2).exceptAll(person2.id.plus(1).where(person2.name.eq(employee.name))).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 EXCEPT ALL SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = employee.id.where(person.id.plus(2).exceptDistinct(person2.id.plus(1).where(person2.name.eq(employee.name))).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 EXCEPT DISTINCT SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }


    public void testIntersect() throws Exception {
        final String sql = employee.id.where(person.id.plus(2).intersect(person2.id.plus(1).where(person2.name.eq(employee.name))).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 INTERSECT SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = employee.id.where(person.id.plus(2).intersectAll(person2.id.plus(1).where(person2.name.eq(employee.name))).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 INTERSECT ALL SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = employee.id.where(person.id.plus(2).intersectDistinct(person2.id.plus(1).where(person2.name.eq(employee.name))).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1 INTERSECT DISTINCT SELECT T2.id + ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.id.plus(2).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id + ? FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.plus(2).forUpdate().show();
        assertSimilar("SELECT T1.id + ? AS C1 FROM person AS T1 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.plus(2).forReadOnly().show();
        assertSimilar("SELECT T1.id + ? AS C1 FROM person AS T1 FOR READ ONLY", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.id.plus(2).queryValue().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT(SELECT T0.id + ? FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(person.id.plus(2)).show();
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.id + ? END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.id.plus(1)).orElse(person.id.plus(2)).show();
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.id + ? ELSE T0.id + ? END AS C0 FROM person AS T0", sql);
    }


    public void testLike() throws Exception {
        final String sql = person.id.where(person.id.plus(1).like(DynamicParameter.create(Mappers.STRING, "12%"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.id.plus(1).notLike(DynamicParameter.create(Mappers.STRING, "12%"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? NOT LIKE ?", sql);
    }

    public void  testLikeString() throws Exception {
        final String sql = person.id.where(person.id.plus(1).like("12%")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? LIKE ?", sql);
    }

    public void  testNotLikeString() throws Exception {
        final String sql = person.id.where(person.id.plus(1).notLike("12%")).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? NOT LIKE ?", sql);
    }

    public void testCount() throws Exception {
        final String sql = person.id.plus(1).count().show();
        assertSimilar("SELECT COUNT(T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.id.plus(1).countDistinct().show();
        assertSimilar("SELECT COUNT(DISTINCT T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.id.plus(1).sum().show();
        assertSimilar("SELECT SUM(T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.id.plus(1).avg().show();
        assertSimilar("SELECT AVG(T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.id.plus(1).min().show();
        assertSimilar("SELECT MIN(T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.id.plus(1).max().show();
        assertSimilar("SELECT MAX(T1.id + ?) AS C1 FROM person AS T1", sql);
    }

    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractNumericExpression<Number> numericExpression) throws SQLException {
                final List<Number> list = numericExpression.list(datasource);
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();

        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractNumericExpression<Number> numericExpression) throws SQLException {
                final List<Number> list = numericExpression.list(new DialectDataSource(GenericDialect.get(), datasource));
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();
    }


    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractNumericExpression<Number> numericExpression) throws SQLException {
                numericExpression.scroll(datasource, new Callback<Number>() {
                    int callCount = 0;

                    @Override
                    public boolean iterate(final Number aNumber) {
                        if (callCount++ != 0) {
                            fail("One call expected, actually " + callCount);
                        }
                        assertEquals(123L, aNumber.longValue());
                        return true;
                    }
                });
            }
        }.play();

        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractNumericExpression<Number> numericExpression) throws SQLException {
                numericExpression.scroll(new DialectDataSource(GenericDialect.get(), datasource), new Callback<Number>() {
                    int callCount = 0;

                    @Override
                    public boolean iterate(final Number aNumber) {
                        if (callCount++ != 0) {
                            fail("One call expected, actually " + callCount);
                        }
                        assertEquals(123L, aNumber.longValue());
                        return true;
                    }
                });
            }
        }.play();
    }

    private static abstract class Scenario {
        public void play() throws Exception {
            final DataSource datasource = createMock(DataSource.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            final AbstractNumericExpression<Number> numericExpression = person.id.plus(two);
            final String queryString = numericExpression.show();
            expect(datasource.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            statement.setLong(1, 2L);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getBigDecimal(matches("C[0-9]"))).andReturn(new BigDecimal(123));
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(datasource, connection, statement, resultSet);

            runQuery(datasource, numericExpression);
            verify(datasource, connection, statement, resultSet);

        }

        protected abstract void runQuery(final DataSource datasource, final AbstractNumericExpression<Number> numericExpression) throws SQLException;
    }



    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }
    
    private static Person person = new Person();
    private static Person person2 = new Person();
    
    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Employee employee = new Employee();
    
    private static DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
