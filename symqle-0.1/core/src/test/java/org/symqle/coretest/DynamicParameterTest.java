package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.MalformedStatementException;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Label;
import org.symqle.sql.SqlFunction;
import org.symqle.sql.TableOrView;
import org.symqle.sql.ValueExpression;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 21.11.12
 * Time: 20:50
 * To change this template use File | Settings | File Templates.
 */
public class DynamicParameterTest extends SqlTestCase {


    public void testSelectStatementNoFrom() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
        try {
            param.showQuery(new GenericDialect(), Option.allowNoTables(true));
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testOracleLikeDialect() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT ? AS C0 FROM dual AS T0", sql);
    }



    public void testSelectAll() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.selectAll().where(id.eq(param)).showQuery(new GenericDialect());
        assertSimilar("SELECT ALL ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testSelectDistinct() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.distinct().where(id.eq(param)).showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.pair(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? AS C1, T1.id AS C2 FROM person AS T1", sql);
    }

    public void testWhere() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.where(id.eq(param)).showQuery(new GenericDialect());
        assertSimilar("SELECT ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testAsFunctionArgument() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final Column<Long> id = person.id;
        final String sql = SqlFunction.create("abs", CoreMappers.LONG).apply(param).where(id.asPredicate()).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(?) AS C1 FROM person AS T1 WHERE T1.id", sql);
    }

    public void testAsCondition() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.asPredicate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ?", sql);
    }

    public void testEq() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.eq(id)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? = T0.id", sql);
    }

    public void testNe() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.ne(id)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <> T0.id", sql);
    }

    public void testGt() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.gt(id)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? > T0.id", sql);
    }

    public void testGe() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.ge(id)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? >= T0.id", sql);
    }

    public void testLt() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.lt(id)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? < T0.id", sql);
    }

    public void testLe() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.le(id)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <= T0.id", sql);
    }

    public void testEqValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.eq(2L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? = ?", sql);
    }


    public void testNeValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.ne(2L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <> ?", sql);
    }


    public void testLtValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.lt(2L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? < ?", sql);
    }


    public void testLeValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.le(2L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <= ?", sql);
    }


    public void testGtValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.gt(2L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? > ?", sql);
    }


    public void testGeValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.where(param.ge(2L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? >= ?", sql);
    }

    public void testAsInSubquery() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            String sql = id.where(id.in(param)).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals(e.getMessage(), "At least one table is required for FROM clause");
        }
    }



    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final Column<Long> id2 = employee.id;
        String sql = id.where(param.in(id2)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testNotInAll() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final Column<Long> id2 = employee.id;
        String sql = id.where(param.notIn(id2)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        String sql = id.where(param.in(1L, 2L, 3L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);

        final ValueExpression<Long> expr = DynamicParameter.create(CoreMappers.LONG, 1L);
        final ValueExpression<Long> expr2 = DynamicParameter.create(CoreMappers.LONG, 2L);
        final ValueExpression<Long> expr3 = DynamicParameter.create(CoreMappers.LONG, 3L);
        String sql = id.where(param.notIn(1L, 2L, 3L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        String sql = id.where(param.isNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        String sql = id.where(param.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IS NOT NULL", sql);
   }

    public void testLabel() throws Exception {
        final Column<Long> id = person.id;
        final Label l = new Label();
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.label(l).orderBy(l, id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? AS C1 FROM person AS T1 ORDER BY C1, T1.id", sql);
    }

    public void testOrderBy() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.orderBy(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? AS C1 FROM person AS T1 ORDER BY T1.id", sql);
    }

    public void testAsSortSpecification() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.orderBy(param).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ?", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.orderBy(param.nullsFirst()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.orderBy(param.nullsLast()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.orderBy(param.desc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = id.orderBy(param.asc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? ASC", sql);
    }

    public void testMult() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.mult(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? * T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.mult(5).add(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? * ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        String sql = param.opposite().where(id.asPredicate()).showQuery(new GenericDialect());
        assertSimilar("SELECT - ? AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testCast() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        String sql = param.cast("INTEGER").where(id.asPredicate()).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(? AS INTEGER) AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testAdd() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.add(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.add(2).add(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? + ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testParamWithValue() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.add(param.param(2L)).add(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? + ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testParamWithoutValue() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.add(param.param()).add(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? + ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.sub(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.sub(1.3).add(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? - ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.div(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? / T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.div(4L).add(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? / ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.concat(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = DynamicParameter.create(CoreMappers.STRING, "abcd").substring(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = DynamicParameter.create(CoreMappers.STRING, "abcd").substring(person.id, person.id.div(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = DynamicParameter.create(CoreMappers.STRING, "abcd").substring(2).pair(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? FROM ?) AS C0, T0.id AS C1 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = DynamicParameter.create(CoreMappers.STRING, "abcd").substring(2, 5).pair(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? FROM ? FOR ?) AS C0, T0.id AS C1 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        final String sql = DynamicParameter.create(CoreMappers.STRING, "abcd").positionOf(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.name IN ?) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        final String sql = DynamicParameter.create(CoreMappers.STRING, "abcd").positionOf("bc").pair(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN ?) AS C0, T0.id AS C1 FROM person AS T0", sql);
    }

    public void testCollate() throws Exception {
        final DynamicParameter<String> param = DynamicParameter.create(CoreMappers.STRING, "abc ");
        final String sql = param.collate("latin1_general_ci").concat(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT ? COLLATE latin1_general_ci || T0.name AS C0 FROM person AS T0", sql);
    }

    public void testString() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.concat(" ").concat(id).showQuery(new GenericDialect());
        assertSimilar("SELECT ? || ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.union(person.id).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testUnionOracleLikeDialect() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.union(person.id).showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
//        assertSimilar("SELECT ? AS C0 FROM dual AS D727817162 UNION SELECT T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMySqlLikeDialect() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.union(person.id).showQuery(new MysqlLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT ? AS C0 UNION SELECT T0.id AS C0 FROM person AS T0", sql);
    }

    public void testUnionAll() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.unionAll(person.id).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testUnionDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.unionDistinct(person.id).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExcept() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.except(person.id).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExceptDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.exceptDistinct(person.id).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExceptAll() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.exceptAll(person.id).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersect() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.intersect(person.id).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersectAll() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.intersectAll(person.id).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersectDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.intersectDistinct(person.id).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExists() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            person.id.where(param.exists()).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testConains() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            person.id.where(param.contains(1L)).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testQueryValue() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            person.id.where(param.queryValue().eq(param)).showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testCount() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.count().where(person.id.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }

    public void testCountDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.countDistinct().where(person.id.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT ?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }

    public void testAvg() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.avg().where(person.id.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT AVG(?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }

    public void testSum() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.sum().where(person.id.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT SUM(?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }

    public void testMin() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.min().where(person.id.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT MIN(?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }

    public void testMax() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        final String sql = param.max().where(person.id.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT MAX(?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }





    public void testForUpdate() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.forUpdate().showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testLimit() throws Exception {
        final String sql = DynamicParameter.create(CoreMappers.LONG, 1L).limit(20).showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT ? AS C0 FROM dual AS T0 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = DynamicParameter.create(CoreMappers.LONG, 1L).limit(10, 20).showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT ? AS C0 FROM dual AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testForReadOnly() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(CoreMappers.LONG, 1L);
        try {
            param.forReadOnly().showQuery(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(DynamicParameter.create(CoreMappers.STRING, "John").like(person.name)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? LIKE T0.name", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(DynamicParameter.create(CoreMappers.STRING, "John").like("J%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(DynamicParameter.create(CoreMappers.STRING, "John").notLike(person.name)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT LIKE T0.name", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(DynamicParameter.create(CoreMappers.STRING, "John").notLike("J%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT LIKE ?", sql);
    }



    public void testList() throws Exception {
        new Scenario(DynamicParameter.create(CoreMappers.LONG, 123L)) {
            @Override
            void use(DynamicParameter<Long> query, QueryEngine engine) throws SQLException {
                final List<Long> expected = Arrays.asList(123L);
                assertEquals(expected, query.list(engine, Option.allowNoTables(true)));
            }
        }.play();
    }


    public void testScroll() throws Exception {
        new Scenario(DynamicParameter.create(CoreMappers.LONG, 123L)) {
            @Override
            void use(DynamicParameter<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Long>(123L), Option.allowNoTables(true)));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(DynamicParameter.create(CoreMappers.LONG, 123L)) {
            @Override
            void use(DynamicParameter<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine, Option.allowNoTables(true)).scroll(new TestCallback<Long>(123L)));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<Long, DynamicParameter<Long>> {

        private Scenario(DynamicParameter<Long> query) {
            super(query, "C[0-9]", new OracleLikeDialect(), Option.allowNoTables(true));
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            OutBox parameter = createMock(OutBox.class);
            expect(parameters.next()).andReturn(parameter);
            parameter.setLong(123L);
            return Collections.singletonList(parameter);
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getLong()).andReturn(123L);
        }
    }

    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<Long> age = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<Long> parentId = defineColumn(CoreMappers.LONG, "parent_id");
    }

    private static class Employee extends TableOrView {
        @Override
        public String getTableName() {
            return "employee";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
    }

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

}
