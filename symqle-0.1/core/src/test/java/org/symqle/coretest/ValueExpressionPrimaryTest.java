package org.symqle.coretest;

import org.symqle.common.Element;
import org.symqle.common.MalformedStatementException;
import org.symqle.common.Mappers;
import org.symqle.common.SqlParameter;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.expect;

/**
 * @author lvovich
 */
public class ValueExpressionPrimaryTest extends SqlTestCase {

    // show is not applicable to subquery
    public void testShow() throws Exception {
        {
            try {
                final AbstractQuerySpecificationScalar<Long> querySpecificationScalar = person.id.where(person.name.eq(employee.name));
                final String sql = querySpecificationScalar.queryValue().show(new GenericDialect());
                fail("expected MalformedStatementException but was " + sql);
            } catch (MalformedStatementException e) {
                assertTrue(e.getMessage(), e.getMessage().startsWith("Implicit cross joins are not allowed"));
                System.out.println(e.getMessage());
            }
        }
        {
            try {
                final String sql = person.id.where(person.name.eq(employee.name)).queryValue().show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
                fail("expected MalformedStatementException but was " + sql);
            } catch (MalformedStatementException e) {
                assertEquals("At least one table is required for FROM clause", e.getMessage());
            }
        }
    }

    public void testMap() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().map(Mappers.STRING).orderBy(employee.name).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T4.id FROM person AS T4 WHERE T4.name = T3.name) AS C1 FROM employee AS T3 ORDER BY T3.name", sql);
    }

    public void testAll() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().selectAll().orderBy(employee.name).show(new GenericDialect());
        assertSimilar("SELECT ALL(SELECT T4.id FROM person AS T4 WHERE T4.name = T3.name) AS C1 FROM employee AS T3 ORDER BY T3.name", sql);
    }

    public void testDistinct() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().distinct().orderBy(employee.name).show(new GenericDialect());
        assertSimilar("SELECT DISTINCT(SELECT T4.id FROM person AS T4 WHERE T4.name = T3.name) AS C1 FROM employee AS T3 ORDER BY T3.name", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().eq(employee.id).asValue().show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) = T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().ne(employee.id).asValue().show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <> T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().gt(employee.id).asValue().show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) > T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().ge(employee.id).asValue().show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) >= T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().lt(employee.id).asValue().show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) < T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().le(employee.id).asValue().show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <= T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().eq(1L).asValue().orderBy(employee.name).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) = ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().ne(1L).asValue().orderBy(employee.name).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <> ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().gt(1L).asValue().orderBy(employee.name).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) > ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().ge(1L).asValue().orderBy(employee.name).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) >= ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().lt(1L).asValue().orderBy(employee.name).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) < ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().le(1L).asValue().orderBy(employee.name).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <= ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testPair() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().pair(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0, T1.id AS C1 FROM employee AS T1", sql);
    }
    public void testAdd() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().add(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testSub() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().sub(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) - T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testMult() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().mult(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) * T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testDiv() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().div(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) / T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testAddNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().add(2).add(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) + ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testSubNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().sub(2).add(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) - ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().mult(2).add(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) * ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().div(2).add(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) / ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testConcat() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().concat(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) || T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().substring(employee.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name) FROM T1.id) AS C0 FROM employee AS T1", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().substring(employee.id, employee.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name) FROM T1.id FOR T1.id / ?) AS C0 FROM employee AS T1", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().substring(2).pair(employee.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name) FROM ?) AS C0, T1.id AS C1 FROM employee AS T1", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().substring(2, 5).pair(employee.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name) FROM ? FOR ?) AS C0, T1.id AS C1 FROM employee AS T1", sql);
    }

    public void testPosition() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().positionOf(employee.name).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T1.name IN(SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1", sql);
    }

    public void testPositionParam() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().positionOf("e").pair(employee.id).show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN(SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name)) AS C0, T1.id AS C1 FROM employee AS T1", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().cast("CHAR(10)").orderBy(employee.id).show(new GenericDialect());
        assertSimilar("SELECT CAST((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS CHAR(10)) AS C0 FROM employee AS T1 ORDER BY T1.id", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().concat(":").concat(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) || ? || T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testCollate() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().collate("latin1_general_ci").concat(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) COLLATE latin1_general_ci || T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testForReadOnly() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().forReadOnly().show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail("MalformedStatementException expected but produced: " + sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testForUpdate() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().forUpdate().show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testOrderBy() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().orderBy(employee.id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0 FROM employee AS T1 ORDER BY T1.id", sql);
    }

    public void testAsSortSpecification() throws Exception {
        final String sql = employee.id.orderBy(person.id.where(person.name.eq(employee.name)).queryValue()).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)", sql);
    }

    public void testAsc() throws Exception {
        final String sql = employee.id.orderBy(person.id.where(person.name.eq(employee.name)).queryValue().asc()).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) ASC", sql);
    }

    public void testDesc() throws Exception {
        final String sql = employee.id.orderBy(person.id.where(person.name.eq(employee.name)).queryValue().desc()).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) DESC", sql);
    }

    public void testNullsFirst() throws Exception {
        final String sql = employee.id.orderBy(person.id.where(person.name.eq(employee.name)).queryValue().nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) NULLS FIRST", sql);
    }

    public void testNullsLast() throws Exception {
        final String sql = employee.id.orderBy(person.id.where(person.name.eq(employee.name)).queryValue().nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) NULLS LAST", sql);
    }

    public void testUnion() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().union(employee.id).show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testUnionAll() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().unionAll(employee.id).show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testUnionDistinct() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().unionDistinct(employee.id).show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }


    public void testExcept() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().except(employee.id).show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExceptAll() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().exceptAll(employee.id).show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExceptDistinct() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().exceptDistinct(employee.id).show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersect() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().intersect(employee.id).show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersectAll() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().intersectAll(employee.id).show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersectDistinct() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().intersectDistinct(employee.id).show(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExists() throws Exception {
        try {
            final String sql = employee.id.where(person.id.where(person.name.eq(employee.name)).queryValue().exists()).show(new GenericDialect());
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testAsInArgument() throws Exception {
        try {
            final AbstractValueExpressionPrimary<Long> vep = person.id.where(person.name.eq(employee.name)).queryValue();
            final String sql = employee.id.where(employee.id.in(vep)).show(new GenericDialect());
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testContains() throws Exception {
        try {
            final AbstractValueExpressionPrimary<Long> vep = person.id.where(person.name.eq(employee.name)).queryValue();
            final String sql = employee.id.where(vep.contains(1L)).show(new GenericDialect());
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testQueryValue() throws Exception {
        try {
            final String sql = person.id.where(person.name.eq(employee.name)).queryValue().queryValue().where(employee.id.eq(1L)).show(new GenericDialect());
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(employee.name.where(employee.id.eq(person.id)).queryValue()).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.name).orElse(employee.name.where(employee.id.eq(person.id)).queryValue()).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name ELSE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) END AS C0 FROM person AS T0", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().like(DynamicParameter.create(Mappers.STRING, "true"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().notLike(DynamicParameter.create(Mappers.STRING, "true"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().like("true")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().notLike("true")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) NOT LIKE ?", sql);
    }


    public void testList() throws Exception {
        new Scenario(person.id.queryValue()) {
            @Override
            void use(AbstractValueExpressionPrimary<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(Arrays.asList(123L), query.list(engine, Option.allowNoTables(true)));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(person.id.queryValue()) {
            @Override
            void use(AbstractValueExpressionPrimary<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Long>(123L), Option.allowNoTables(true)));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<Long, AbstractValueExpressionPrimary<Long>> {
        private Scenario(AbstractValueExpressionPrimary<Long> query) {
            super(query, "C[0-9]", new OracleLikeDialect(), Option.allowNoTables(true));
        }

        @Override
        List<SqlParameter> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(Element element) throws SQLException {
            expect(element.getLong()).andReturn(123L);
        }
    }

    public void testCount() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().count().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT COUNT((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().countDistinct().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().avg().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT AVG((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().sum().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT SUM((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().min().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT MIN((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.id.where(person.name.eq(employee.name)).queryValue().max().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT MAX((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }



    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();
    private static Employee employee = new Employee();

}
