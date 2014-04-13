package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractCastSpecification;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Label;
import org.symqle.sql.SqlFunction;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.expect;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 21.11.12
 * Time: 20:50
 * To change this template use File | Settings | File Templates.
 */
public class CastSpecificationTest extends SqlTestCase {


    private AbstractCastSpecification<Long> createCast() {
        return person.id.cast("NUMBER(12,0)");
    }

    public void testShow() throws Exception {
        final AbstractCastSpecification<Long> col = createCast();
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C1 FROM person AS T1", col.showQuery(new GenericDialect()));
    }

    public void testAdapt() throws Exception {
        final AbstractCastSpecification<Long> cast = createCast();
        final String sql1 = cast.showQuery(new GenericDialect());
        final AbstractCastSpecification<Long> adaptor = AbstractCastSpecification.adapt(cast);
        final String sql2 = adaptor.showQuery(new GenericDialect());
        assertEquals(sql1, sql2);
        assertEquals(cast.getMapper(), adaptor.getMapper());
    }

    public void testAsInValueList() throws Exception {
        final AbstractCastSpecification<Long> cast = createCast();
        final String sql = person.name.where(person.parentId.in(cast.asInValueList())).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE T1.parent_id IN(CAST(T1.id AS NUMBER(12,0)))", sql);
    }

    public void testCollate() throws Exception {
        final String sql = person.name.cast("CHAR(10)").collate("latin1_general_ci").showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T1.name AS CHAR(10)) COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testMap() throws Exception {
        final AbstractCastSpecification<Long> col = createCast();
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C1 FROM person AS T1", col.map(CoreMappers.LONG).showQuery(new GenericDialect()));
    }

    public void testSelectStatementFunctionality() throws Exception {
        final AbstractCastSpecification<Long> col = createCast();
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0", col.showQuery(new GenericDialect()));
    }

    public void testSelectAll() throws Exception {
        final AbstractCastSpecification<Long> col = createCast();
        assertSimilar("SELECT ALL CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0", col.selectAll().showQuery(new GenericDialect()));

    }

    public void testSelectDistinct() throws Exception {
        final AbstractCastSpecification<Long> col = createCast();
        assertSimilar("SELECT DISTINCT CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0", col.distinct().showQuery(new GenericDialect()));
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = SqlFunction.create("abs", CoreMappers.LONG).apply(person.id.cast("NUMBER(12,0)")).showQuery(new GenericDialect());
        assertSimilar("SELECT abs(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testAsFunctionMultipleArguments() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final String sql = SqlFunction.create("max", CoreMappers.LONG).apply(column, column).showQuery(new GenericDialect());
        assertSimilar("SELECT max(CAST(T1.id AS NUMBER(12,0)), CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final AbstractCastSpecification<Long> id = createCast();
        final String sql = id.where(person.id.asBoolean()).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testEq() throws Exception {
        final AbstractCastSpecification<Long> id = createCast();
        final Column<Long> age = person.age;
        final String sql = person.id.where(id.eq(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) = T0.age", sql);
    }

    public void testNe() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.ne(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.gt(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) > T0.age", sql);
    }

    public void testGe() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.ge(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.lt(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) < T0.age", sql);
    }

    public void testLe() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.le(age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <= T0.age", sql);
    }

    public void testEqValue() throws Exception {
        final AbstractCastSpecification<Long> id = createCast();
        final String sql = person.id.where(id.eq(1L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final AbstractCastSpecification<Long> id = createCast();
        final String sql = person.id.where(id.ne(1L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <> ?", sql);
    }

    public void testLtValue() throws Exception {
        final AbstractCastSpecification<Long> id = createCast();
        final String sql = person.id.where(id.lt(1L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final AbstractCastSpecification<Long> id = createCast();
        final String sql = person.id.where(id.le(1L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <= ?", sql);
    }

    public void testGtValue() throws Exception {
        final AbstractCastSpecification<Long> id = createCast();
        final String sql = person.id.where(id.gt(1L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final AbstractCastSpecification<Long> id = createCast();
        final String sql = person.id.where(id.ge(1L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) >= ?", sql);
    }

    public void testExceptAll() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final String sql = column.exceptAll(person2.age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final String sql = column.exceptDistinct(person.age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final String sql = column.except(person2.age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 EXCEPT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final String sql = column.unionAll(person2.age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 UNION ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final String sql = column.unionDistinct(person2.age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final String sql = column.union(person2.age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 UNION SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final String sql = column.intersectAll(person2.age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersectDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final String sql = column.intersectDistinct(person2.age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final String sql = column.intersect(person2.age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testUseSameTableInDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = createCast();
        final Column<Long> age = person.age;
        final String sql = column.intersectDistinct(age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testSelectForUpdate() throws Exception {
        final AbstractCastSpecification<Long> col = createCast();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 FOR UPDATE", col.forUpdate().showQuery(new GenericDialect()));
    }

    public void testSelectForReadOnly() throws Exception {
        final AbstractCastSpecification<Long> col = createCast();
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 FOR READ ONLY", col.forReadOnly().showQuery(new GenericDialect()));
    }

    public void testExists() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final AbstractCastSpecification<Long> age2 = person2.age.cast("FLOAT");
        String sql = id.where(age2.exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT CAST(T1.age AS FLOAT) FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final AbstractCastSpecification<Long> age2 = person2.age.cast("FLOAT");
        String sql = id.where(age2.contains(20L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT CAST(T1.age AS FLOAT) FROM person AS T1)", sql);
    }

    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.cast("NUMBER(12,0)").in(id2)).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE CAST(T1.id AS NUMBER(12,0)) IN(SELECT T2.id FROM employee AS T2)", sql);
    }

    public void testInArgument() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.in(id2.cast("NUMBER(12,0)"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id IN(SELECT CAST(T2.id AS NUMBER(12,0)) FROM employee AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.cast("NUMBER(12,0)").notIn(id2)).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE CAST(T1.id AS NUMBER(12,0)) NOT IN(SELECT T2.id FROM employee AS T2)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(id.cast("NUMBER(12,0)").in(1L, 2L, 3L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(id.cast("NUMBER(12,0)").notIn(1L, 2L, 3L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(age.cast("NUMBER(12,0)").isNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.age AS NUMBER(12,0)) IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(age.cast("NUMBER(12,0)").isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.age AS NUMBER(12,0)) IS NOT NULL", sql);
   }

    public void testLabel() throws Exception {
        Label l = new Label();
        String sql = person.id.cast("NUMBER(12,0)").label(l).orderBy(l).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 ORDER BY C0", sql);
    }


    public void testOrderBy() throws Exception {
        final Column<Long> age = person.age;
        String sql = person.id.cast("NUMBER(12,0)").orderBy(age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 ORDER BY T0.age", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER(12,0)").nullsFirst()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER(12,0)").nullsLast()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER(12,0)").desc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER(12,0)").asc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) ASC", sql);
    }

    public void testSortSpecification() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER(12,0)")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0))", sql);
    }

    public void testMult() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        String sql = id.mult(age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER(12,0)").pair(age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0, T0.age AS C1 FROM person AS T0", sql);
    }

    public void testFunction() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        SqlFunction<Long> sumOf = SqlFunction.create("SUM_OF", CoreMappers.LONG);
        String sql = sumOf.apply(id, age).showQuery(new GenericDialect());
        assertSimilar("SELECT SUM_OF(T0.id, T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("NUMBER(12,0)").opposite().showQuery(new GenericDialect());
        assertSimilar("SELECT - CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER(12,0)").add(age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) + T0.age AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("NUMBER(12,0)").add(1).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithValue() throws Exception {
        final Column<Long> id  =  person.id;
        final AbstractCastSpecification<Long> cast = id.cast("NUMBER(12,0)");
        String sql = cast.add(cast.param(1L)).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithNoValue() throws Exception {
        final Column<Long> id  =  person.id;
        final AbstractCastSpecification<Long> cast = id.cast("NUMBER(12,0)");
        String sql = cast.add(cast.param()).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) + ? AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER(12,0)").sub(age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) - T0.age AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("NUMBER(12,0)").sub(1.0).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("NUMBER(12,0)").mult(2L).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER(12,0)").div(age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) / T0.age AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER(12,0)").div(3).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("CHAR(12)").concat(age).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS CHAR(12)) || T0.age AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("CHAR(12)").concat(" (id)").showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS CHAR(12)) || ? AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("CHAR(12)").substring(age).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CAST(T0.id AS CHAR(12)) FROM T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testCharLength() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("CHAR(12)").charLength().showQuery(new GenericDialect());
        assertSimilar("SELECT CHAR_LENGTH(CAST(T0.id AS CHAR(12))) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("CHAR(12)").substring(3).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CAST(T0.id AS CHAR(12)) FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("CHAR(12)").substring(age, age.div(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CAST(T0.id AS CHAR(12)) FROM T0.age FOR T0.age / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("CHAR(12)").substring(2, 5).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CAST(T0.id AS CHAR(12)) FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("CHAR(12)").positionOf(person.age).showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.age IN CAST(T0.id AS CHAR(12))) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("CHAR(12)").positionOf("12").showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN CAST(T0.id AS CHAR(12))) AS C0 FROM person AS T0", sql);
    }

    public void testCount() throws Exception {
        final String sql = createCast().count().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.parentId.cast("NUMBER(12,0)").countDistinct().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT CAST(T0.parent_id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.name.cast("CHAR(12)").notLike(DynamicParameter.create(CoreMappers.STRING, "John%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.name AS CHAR(12)) NOT LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.name.cast("CHAR(12)").notLike("John%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.name AS CHAR(12)) NOT LIKE ?", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(person.name.cast("CHAR(12)").like(DynamicParameter.create(CoreMappers.STRING, "John%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.name AS CHAR(12)) LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(person.name.cast("CHAR(12)").like("John%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.name AS CHAR(12)) LIKE ?", sql);
    }

    public void testAvg() throws Exception {
        final String sql = createCast().avg().showQuery(new GenericDialect());
        assertSimilar("SELECT AVG(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testSum() throws Exception {
        final String sql = createCast().sum().showQuery(new GenericDialect());
        assertSimilar("SELECT SUM(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testMax() throws Exception {
        final String sql = createCast().max().showQuery(new GenericDialect());
        assertSimilar("SELECT MAX(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testMin() throws Exception {
        final String sql = createCast().min().showQuery(new GenericDialect());
        assertSimilar("SELECT MIN(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(person.name.cast("BOOLEAN").asBoolean()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.name AS BOOLEAN)", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.id.cast("NUMBER").cast("NUMBER(12,0)").showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(CAST(T0.id AS NUMBER) AS NUMBER(12,0)) AS C0 FROM person AS T0", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.id.cast("NUMBER").queryValue().where(employee.id.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT CAST(T0.id AS NUMBER) FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.id IS NOT NULL", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.id).orElse(person.id.cast("NUMBER")).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.id ELSE CAST(T0.id AS NUMBER) END AS C0 FROM person AS T0", sql);
    }

    public void testLimit() throws Exception {
        final String sql = createCast().limit(10).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C1 FROM person AS T1 FETCH FIRST 10 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createCast().limit(1, 2).showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C1 FROM person AS T1 OFFSET 1 ROWS FETCH FIRST 2 ROWS ONLY", sql);
    }



    public void testList() throws Exception {
        new Scenario(createCast()) {
            @Override
            void use(AbstractCastSpecification<Long> query, QueryEngine engine) throws SQLException {
                final List<Long> expected = Arrays.asList(123L);
                final List<Long> list = query.list(engine);
                assertEquals(expected, list);
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(createCast()) {
            @Override
            void use(AbstractCastSpecification<Long> query, QueryEngine engine) throws SQLException {
                int rows = query.scroll(
                        engine,
                    new TestCallback<Long>(123L));
                assertEquals(1, rows);
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(createCast()) {
            @Override
            void use(AbstractCastSpecification<Long> query, QueryEngine engine) throws SQLException {
                int rows = query.compileQuery(engine).scroll(
                    new TestCallback<Long>(123L));
                assertEquals(1, rows);
            }
        }.play();
    }

    private abstract class Scenario extends AbstractQueryScenario<Long, AbstractCastSpecification<Long>> {

        protected Scenario(AbstractCastSpecification<Long> query) {
            super(query);
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

    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<Long> age = defineColumn(CoreMappers.LONG, "age");
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
