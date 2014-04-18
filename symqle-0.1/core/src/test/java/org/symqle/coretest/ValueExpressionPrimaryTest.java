package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.MalformedStatementException;
import org.symqle.common.OutBox;
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

    private AbstractValueExpressionPrimary<Long> createValueExpressionPrimary() {
        return person.id.where(person.name.eq(employee.name)).queryValue();
    }

    public void testShow() throws Exception {
    // show is not applicable to subquery in GenericDialect
        {
            try {
                final AbstractQuerySpecificationScalar<Long> querySpecificationScalar = person.id.where(person.name.eq(employee.name));
                final String sql = querySpecificationScalar.queryValue().showQuery(new GenericDialect());
                fail("expected MalformedStatementException but was " + sql);
            } catch (MalformedStatementException e) {
                assertTrue(e.getMessage(), e.getMessage().startsWith("Implicit cross joins are not allowed"));
                System.out.println(e.getMessage());
            }
        }
        {
            try {
                final String sql = createValueExpressionPrimary().showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
                fail("expected MalformedStatementException but was " + sql);
            } catch (MalformedStatementException e) {
                assertEquals("At least one table is required for FROM clause", e.getMessage());
            }
        }

        final AbstractValueExpressionPrimary<Long> valueExpressionPrimary = person.id.where(person.name.isNotNull()).queryValue();
        final String sql = valueExpressionPrimary.showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name IS NOT NULL) AS C1 FROM dual AS T1", sql);

    }

    public void testAdapt() throws Exception {
        final AbstractValueExpressionPrimary<Number> adaptor = AbstractValueExpressionPrimary.adapt(person.id.mult(2));
        final String sql = adaptor.showQuery(new GenericDialect());
        assertSimilar("SELECT(T1.id * ?) AS C1 FROM person AS T1", sql);
        assertEquals(CoreMappers.NUMBER, adaptor.getMapper());
    }

    public void testMap() throws Exception {
        final AbstractValueExpressionPrimary<String> remapped = createValueExpressionPrimary().map(CoreMappers.STRING);
        final String sql = remapped.orderBy(employee.name).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T4.id FROM person AS T4 WHERE T4.name = T3.name) AS C1 FROM employee AS T3 ORDER BY T3.name", sql);
        assertEquals(CoreMappers.STRING, remapped.getMapper());
    }

    public void testSelectAll() throws Exception {
        final String sql = createValueExpressionPrimary().selectAll().orderBy(employee.name).showQuery(new GenericDialect());
        assertSimilar("SELECT ALL(SELECT T4.id FROM person AS T4 WHERE T4.name = T3.name) AS C1 FROM employee AS T3 ORDER BY T3.name", sql);
    }

    public void testDistinct() throws Exception {
        final String sql = createValueExpressionPrimary().distinct().orderBy(employee.name).showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT(SELECT T4.id FROM person AS T4 WHERE T4.name = T3.name) AS C1 FROM employee AS T3 ORDER BY T3.name", sql);
    }

    public void testWhere() throws Exception {
        final String sql = createValueExpressionPrimary().where(employee.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testAsInValueList() throws Exception {
        final String sql = employee.id.in(createValueExpressionPrimary().asInValueList()).asValue().showQuery(new GenericDialect());
        // double parentheses expected
        assertSimilar("SELECT T1.id IN((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1", sql);
    }

    public void testEq() throws Exception {
        final String sql = createValueExpressionPrimary().eq(employee.id).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) = T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testEqArg() throws Exception {
        final String sql = employee.id.eq(createValueExpressionPrimary()).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id =(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0 FROM employee AS T1", sql);
    }

    public void testNe() throws Exception {
        final String sql = createValueExpressionPrimary().ne(employee.id).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <> T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testGt() throws Exception {
        final String sql = createValueExpressionPrimary().gt(employee.id).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) > T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testGe() throws Exception {
        final String sql = createValueExpressionPrimary().ge(employee.id).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) >= T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testLt() throws Exception {
        final String sql = createValueExpressionPrimary().lt(employee.id).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) < T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testLe() throws Exception {
        final String sql = createValueExpressionPrimary().le(employee.id).asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <= T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = createValueExpressionPrimary().eq(1L).asValue().orderBy(employee.name).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) = ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = createValueExpressionPrimary().ne(1L).asValue().orderBy(employee.name).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <> ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = createValueExpressionPrimary().gt(1L).asValue().orderBy(employee.name).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) > ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = createValueExpressionPrimary().ge(1L).asValue().orderBy(employee.name).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) >= ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = createValueExpressionPrimary().lt(1L).asValue().orderBy(employee.name).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) < ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = createValueExpressionPrimary().le(1L).asValue().orderBy(employee.name).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) <= ? AS C0 FROM employee AS T1 ORDER BY T1.name", sql);
    }

    public void testPair() throws Exception {
        final String sql = createValueExpressionPrimary().pair(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0, T1.id AS C1 FROM employee AS T1", sql);
    }
    public void testAdd() throws Exception {
        final String sql = createValueExpressionPrimary().add(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testSub() throws Exception {
        final String sql = createValueExpressionPrimary().sub(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) - T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testMult() throws Exception {
        final String sql = createValueExpressionPrimary().mult(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) * T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testDiv() throws Exception {
        final String sql = createValueExpressionPrimary().div(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) / T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testAddNumber() throws Exception {
        final String sql = createValueExpressionPrimary().add(2).add(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) + ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testParamWithValue() throws Exception {
        final AbstractValueExpressionPrimary<Long> valueExpressionPrimary = createValueExpressionPrimary();
        final String sql = valueExpressionPrimary.add(valueExpressionPrimary.param(2L)).add(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) + ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testParamWithoutValue() throws Exception {
        final AbstractValueExpressionPrimary<Long> valueExpressionPrimary = createValueExpressionPrimary();
        final String sql = valueExpressionPrimary.add(valueExpressionPrimary.param()).add(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) + ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testSubNumber() throws Exception {
        final String sql = createValueExpressionPrimary().sub(2).add(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) - ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = createValueExpressionPrimary().mult(2).add(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) * ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = createValueExpressionPrimary().div(2).add(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) / ? + T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testConcat() throws Exception {
        final String sql = createValueExpressionPrimary().concat(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) || T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().substring(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name) FROM T1.id) AS C0 FROM employee AS T1", sql);
    }

    public void testCharLength() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().charLength()
                .where(employee.name.eq("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT CHAR_LENGTH((SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name = ?", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().substring(employee.id, employee.id.div(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name) FROM T1.id FOR T1.id / ?) AS C0 FROM employee AS T1", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().substring(2).pair(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name) FROM ?) AS C0, T1.id AS C1 FROM employee AS T1", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().substring(2, 5).pair(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name) FROM ? FOR ?) AS C0, T1.id AS C1 FROM employee AS T1", sql);
    }

    public void testPosition() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().positionOf(employee.name).showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(T1.name IN(SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1", sql);
    }

    public void testPositionParam() throws Exception {
        final String sql = person.name.where(person.name.eq(employee.name)).queryValue().positionOf("e").pair(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN(SELECT T0.name FROM person AS T0 WHERE T0.name = T1.name)) AS C0, T1.id AS C1 FROM employee AS T1", sql);
    }

    public void testCast() throws Exception {
        final String sql = createValueExpressionPrimary().cast("CHAR(10)").orderBy(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS CHAR(10)) AS C0 FROM employee AS T1 ORDER BY T1.id", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = createValueExpressionPrimary().concat(":").concat(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) || ? || T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testCollate() throws Exception {
        final String sql = createValueExpressionPrimary().collate("latin1_general_ci").concat(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) COLLATE latin1_general_ci || T1.id AS C0 FROM employee AS T1", sql);
    }

    public void testForReadOnly() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().forReadOnly().showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail("MalformedStatementException expected but produced: " + sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testForUpdate() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().forUpdate().showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testLimit() throws Exception {
        final AbstractValueExpressionPrimary<Long> valueExpressionPrimary = person.id.where(person.name.isNotNull()).queryValue();
        final String sql = valueExpressionPrimary.limit(20).showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        System.out.println(sql);
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name IS NOT NULL) AS C0 FROM dual AS T1 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final AbstractValueExpressionPrimary<Long> valueExpressionPrimary = person.id.where(person.name.isNotNull()).queryValue();
        final String sql = valueExpressionPrimary.limit(10, 20).showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name IS NOT NULL) AS C0 FROM dual AS T1 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLabel() throws Exception {
        final Label l = new Label();
        final String sql = createValueExpressionPrimary().label(l).where(employee.id.isNotNull()).orderBy(l).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0 FROM employee AS T1 WHERE T1.id IS NOT NULL ORDER BY C0", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = createValueExpressionPrimary().orderBy(employee.id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) AS C0 FROM employee AS T1 ORDER BY T1.id", sql);
    }

    public void testAsSortSpecification() throws Exception {
        final String sql = employee.id.orderBy(createValueExpressionPrimary()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)", sql);
    }

    public void testAsc() throws Exception {
        final String sql = employee.id.orderBy(createValueExpressionPrimary().asc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) ASC", sql);
    }

    public void testDesc() throws Exception {
        final String sql = employee.id.orderBy(createValueExpressionPrimary().desc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) DESC", sql);
    }

    public void testNullsFirst() throws Exception {
        final String sql = employee.id.orderBy(createValueExpressionPrimary().nullsFirst()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) NULLS FIRST", sql);
    }

    public void testNullsLast() throws Exception {
        final String sql = employee.id.orderBy(createValueExpressionPrimary().nullsLast()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 ORDER BY(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name) NULLS LAST", sql);
    }

    public void testUnion() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().union(employee.id).showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testUnionAll() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().unionAll(employee.id).showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testUnionDistinct() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().unionDistinct(employee.id).showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }


    public void testExcept() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().except(employee.id).showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExceptAll() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().exceptAll(employee.id).showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExceptDistinct() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().exceptDistinct(employee.id).showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersect() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().intersect(employee.id).showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersectAll() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().intersectAll(employee.id).showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersectDistinct() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().intersectDistinct(employee.id).showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExists() throws Exception {
        try {
            final String sql = employee.id.where(createValueExpressionPrimary().exists()).showQuery(new GenericDialect());
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testCountRows() throws Exception {
        final String sql = employee.id.queryValue().countRows().showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT COUNT(*) AS C0 FROM(SELECT(SELECT T1.id FROM employee AS T1) FROM dual AS T2) AS T0", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(employee.id.queryValue().in(person2.id.selectAll())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IN(SELECT ALL T2.id FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(employee.id.queryValue().notIn(person2.id.selectAll())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) NOT IN(SELECT ALL T2.id FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().in(1L, 2L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IN(?, ?)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().notIn(1L, 2L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) NOT IN(?, ?)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().isNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IS NOT NULL", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = employee.id.queryValue().opposite().orderBy(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT -(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testAsInArgument() throws Exception {
        try {
            final AbstractValueExpressionPrimary<Long> vep = createValueExpressionPrimary();
            final String sql = employee.id.where(employee.id.in(vep)).showQuery(new GenericDialect());
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testContains() throws Exception {
        try {
            final AbstractValueExpressionPrimary<Long> vep = createValueExpressionPrimary();
            final String sql = employee.id.where(vep.contains(1L)).showQuery(new GenericDialect());
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
        final AbstractValueExpressionPrimary<Long> vep = createValueExpressionPrimary();
        final String sql = employee.id.where(vep.contains(1L)).showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT(SELECT T1.id FROM person AS T1 WHERE T1.name = T0.name) FROM dual AS T2)", sql);
        System.out.println(sql);
    }

    public void testAll() throws Exception {
        final AbstractValueExpressionPrimary<Long> vep = createValueExpressionPrimary();
        final String sql = employee.id.where(employee.id.lt(vep.all())).showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.id < ALL(SELECT(SELECT T1.id FROM person AS T1 WHERE T1.name = T0.name) FROM dual AS T2)", sql);
        System.out.println(sql);
    }

    public void testAny() throws Exception {
        final AbstractValueExpressionPrimary<Long> vep = createValueExpressionPrimary();
        final String sql = employee.id.where(employee.id.lt(vep.any())).showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.id < ANY(SELECT(SELECT T1.id FROM person AS T1 WHERE T1.name = T0.name) FROM dual AS T2)", sql);
        System.out.println(sql);
    }

    public void testSome() throws Exception {
        final AbstractValueExpressionPrimary<Long> vep = createValueExpressionPrimary();
        final String sql = employee.id.where(employee.id.lt(vep.some())).showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.id < SOME(SELECT(SELECT T1.id FROM person AS T1 WHERE T1.name = T0.name) FROM dual AS T2)", sql);
        System.out.println(sql);
    }

    public void testQueryValue() throws Exception {
        try {
            final String sql = createValueExpressionPrimary().queryValue().where(employee.id.eq(1L)).showQuery(new GenericDialect());
            fail ("MalformedStatementException expected but produced: "+sql);
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(employee.name.where(employee.id.eq(person.id)).queryValue()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.name).orElse(employee.name.where(employee.id.eq(person.id)).queryValue()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name ELSE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) END AS C0 FROM person AS T0", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().like(DynamicParameter.create(CoreMappers.STRING, "true"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().notLike(DynamicParameter.create(CoreMappers.STRING, "true"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().like("true")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.name FROM employee AS T1 WHERE T1.id = T0.id) LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(employee.name.where(employee.id.eq(person.id)).queryValue().notLike("true")).showQuery(new GenericDialect());
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

    public void testCompile() throws Exception {
        new Scenario(person.id.queryValue()) {
            @Override
            void use(AbstractValueExpressionPrimary<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine, Option.allowNoTables(true)).scroll(new TestCallback<Long>(123L)));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<Long, AbstractValueExpressionPrimary<Long>> {
        private Scenario(AbstractValueExpressionPrimary<Long> query) {
            super(query, "C[0-9]", new MysqlLikeDialect(), Option.allowNoTables(true));
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getLong()).andReturn(123L);
        }
    }

    public void testCount() throws Exception {
        final String sql = createValueExpressionPrimary().count().where(employee.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = createValueExpressionPrimary().countDistinct().where(employee.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT(SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testAvg() throws Exception {
        final String sql = createValueExpressionPrimary().avg().where(employee.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT AVG((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testSum() throws Exception {
        final String sql = createValueExpressionPrimary().sum().where(employee.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT SUM((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testMin() throws Exception {
        final String sql = createValueExpressionPrimary().min().where(employee.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT MIN((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testMax() throws Exception {
        final String sql = createValueExpressionPrimary().max().where(employee.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT MAX((SELECT T0.id FROM person AS T0 WHERE T0.name = T1.name)) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }



    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

    private static class Employee extends TableOrView {
        @Override
        public String getTableName() {
            return "employee";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

    private static Person person = new Person();
    private static Person person2 = new Person();
    private static Employee employee = new Employee();

}
