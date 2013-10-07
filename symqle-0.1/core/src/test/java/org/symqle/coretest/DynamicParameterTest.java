package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.MalformedStatementException;
import org.symqle.common.Mappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.Column;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.SqlFunction;
import org.symqle.sql.TableOrView;
import org.symqle.sql.ValueExpression;

import java.util.Collections;

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
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
        try {
            param.show(new GenericDialect(), Option.allowNoTables(true));
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testOracleLikeDialect() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.show(new GenericDialect() {
            @Override
            public String fallbackTableName() {
                return "dual";
            }
        }, Option.allowNoTables(true));
        assertSimilar("SELECT ? AS C1 FROM dual AS T1", sql);
    }


    public void testSelectAll() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.selectAll().where(id.eq(param)).show(new GenericDialect());
        assertSimilar("SELECT ALL ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testMap() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.map(Mappers.INTEGER).selectAll().where(id.eq(param)).show(new GenericDialect());
        assertSimilar("SELECT ALL ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testSelectDistinct() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.distinct().where(id.eq(param)).show(new GenericDialect());
        assertSimilar("SELECT DISTINCT ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.pair(id).show(new GenericDialect());
        assertSimilar("SELECT ? AS C1, T1.id AS C2 FROM person AS T1", sql);
    }

    public void testWhere() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.where(id.eq(param)).show(new GenericDialect());
        assertSimilar("SELECT ? AS C1 FROM person AS T1 WHERE T1.id = ?", sql);
    }

    public void testAsFunctionArgument() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final Column<Long> id = person.id;
        final String sql = SqlFunction.create("abs",Mappers.LONG).apply(param).where(id.asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT abs(?) AS C1 FROM person AS T1 WHERE T1.id", sql);
    }

    public void testAsCondition() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ?", sql);
    }

    public void testEq() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.eq(id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? = T0.id", sql);
    }

    public void testNe() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.ne(id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <> T0.id", sql);
    }

    public void testGt() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.gt(id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? > T0.id", sql);
    }

    public void testGe() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.ge(id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? >= T0.id", sql);
    }

    public void testLt() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.lt(id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? < T0.id", sql);
    }

    public void testLe() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.le(id)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <= T0.id", sql);
    }

    public void testEqValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.eq(2L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? = ?", sql);
    }


    public void testNeValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.ne(2L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <> ?", sql);
    }


    public void testLtValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.lt(2L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? < ?", sql);
    }


    public void testLeValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.le(2L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? <= ?", sql);
    }


    public void testGtValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.gt(2L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? > ?", sql);
    }


    public void testGeValue() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.where(param.ge(2L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? >= ?", sql);
    }

    public void testAsInSubquery() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            String sql = id.where(id.in(param)).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals(e.getMessage(), "At least one table is required for FROM clause");
        }
    }



    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final Column<Long> id2 = employee.id;
        String sql = id.where(param.in(id2)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testNotInAll() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final Column<Long> id2 = employee.id;
        String sql = id.where(param.notIn(id2)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        String sql = id.where(param.in(1L, 2L, 3L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);

        final ValueExpression<Long> expr = DynamicParameter.create(Mappers.LONG, 1L);
        final ValueExpression<Long> expr2 = DynamicParameter.create(Mappers.LONG, 2L);
        final ValueExpression<Long> expr3 = DynamicParameter.create(Mappers.LONG, 3L);
        String sql = id.where(param.notIn(1L, 2L, 3L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        String sql = id.where(param.isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        String sql = id.where(param.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.orderBy(id).show(new GenericDialect());
        assertSimilar("SELECT ? AS C1 FROM person AS T1 ORDER BY T1.id", sql);
    }

    public void testOrderAsc() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            final String sql = param.orderAsc().show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            // expected
        }
    }

    public void testOrderDesc() throws Exception {
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            final String sql = param.orderDesc().show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            // expected
        }
    }

    public void testAsSortSpecification() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.orderBy(param).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ?", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.orderBy(param.nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.orderBy(param.nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.orderBy(param.desc()).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = id.orderBy(param.asc()).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 ORDER BY ? ASC", sql);
    }

    public void testMult() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.mult(id).show(new GenericDialect());
        assertSimilar("SELECT ? * T0.id AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.mult(5).add(id).show(new GenericDialect());
        assertSimilar("SELECT ? * ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        String sql = param.opposite().where(id.asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT - ? AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testCast() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        String sql = param.cast("INTEGER").where(id.asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT CAST(? AS INTEGER) AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testAdd() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.add(id).show(new GenericDialect());
        assertSimilar("SELECT ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.add(2).add(id).show(new GenericDialect());
        assertSimilar("SELECT ? + ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.sub(id).show(new GenericDialect());
        assertSimilar("SELECT ? - T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.sub(1.3).add(id).show(new GenericDialect());
        assertSimilar("SELECT ? - ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.div(id).show(new GenericDialect());
        assertSimilar("SELECT ? / T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.div(4L).add(id).show(new GenericDialect());
        assertSimilar("SELECT ? / ? + T0.id AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.concat(id).show(new GenericDialect());
        assertSimilar("SELECT ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = DynamicParameter.create(Mappers.STRING, "abcd").substring(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = DynamicParameter.create(Mappers.STRING, "abcd").substring(person.id, person.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = DynamicParameter.create(Mappers.STRING, "abcd").substring(2).pair(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? FROM ?) AS C0, T0.id AS C1 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = DynamicParameter.create(Mappers.STRING, "abcd").substring(2, 5).pair(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(? FROM ? FOR ?) AS C0, T0.id AS C1 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        final String sql = DynamicParameter.create(Mappers.STRING, "abcd").positionOf(person.name).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.name IN ?) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        final String sql = DynamicParameter.create(Mappers.STRING, "abcd").positionOf("bc").pair(person.id).show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN ?) AS C0, T0.id AS C1 FROM person AS T0", sql);
    }

    public void testCollate() throws Exception {
        final DynamicParameter<String> param = DynamicParameter.create(Mappers.STRING, "abc ");
        final String sql = param.collate("latin1_general_ci").concat(person.name).show(new GenericDialect());
        assertSimilar("SELECT ? COLLATE latin1_general_ci || T0.name AS C0 FROM person AS T0", sql);
    }

    public void testString() throws Exception {
        final Column<Long> id  =  person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.concat(" ").concat(id).show(new GenericDialect());
        assertSimilar("SELECT ? || ? || T0.id AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.union(person.id).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testUnionAll() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.unionAll(person.id).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testUnionDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.unionDistinct(person.id).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExcept() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.except(person.id).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExceptDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.exceptDistinct(person.id).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExceptAll() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.exceptAll(person.id).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersect() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.intersect(person.id).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersectAll() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.intersectAll(person.id).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testIntersectDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.intersectDistinct(person.id).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testExists() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            person.id.where(param.exists()).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testConains() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            person.id.where(param.contains(1L)).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testQueryValue() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            person.id.where(param.queryValue().eq(param)).show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testCount() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.count().where(person.id.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }

    public void testCountDistinct() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.countDistinct().where(person.id.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT ?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }

    public void testAvg() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.avg().where(person.id.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT AVG(?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }

    public void testSum() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.sum().where(person.id.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT SUM(?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }

    public void testMin() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.min().where(person.id.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT MIN(?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }

    public void testMax() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        final String sql = param.max().where(person.id.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT MAX(?) AS C1 FROM person AS T0 WHERE T0.id IS NOT NULL", sql);
    }





    public void testForUpdate() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.forUpdate().show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testForReadOnly() throws Exception {
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        try {
            param.forReadOnly().show(new GenericDialect());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(DynamicParameter.create(Mappers.STRING, "John").like(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? LIKE T0.name", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(DynamicParameter.create(Mappers.STRING, "John").like("J%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(DynamicParameter.create(Mappers.STRING, "John").notLike(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT LIKE T0.name", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(DynamicParameter.create(Mappers.STRING, "John").notLike("J%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? NOT LIKE ?", sql);
    }



    public void testList() throws Exception {
        final DatabaseGate gate = org.easymock.EasyMock.createMock(DatabaseGate.class);
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
        expect(gate.getDialect()).andReturn(new GenericDialect());
        org.easymock.EasyMock.replay(gate);
        try {
            param.list(gate);
            fail ("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
        org.easymock.EasyMock.verify(gate);
    }

    public void testScroll() throws Exception {
        final DatabaseGate gate = org.easymock.EasyMock.createMock(DatabaseGate.class);
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 1L);
        expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
        expect(gate.getDialect()).andReturn(new GenericDialect());
        org.easymock.EasyMock.replay(gate);
        try {
            param.scroll(gate, new Callback<Long>() {
                @Override
                public boolean iterate(final Long aLong) {
                    fail("Must not get here");
                    return true;
                }
            });
            fail ("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertEquals("At least one table is required for FROM clause", e.getMessage());
        }
        org.easymock.EasyMock.verify(gate);
    }


    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
    }

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

}
