package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.Mappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractCastSpecification;
import org.symqle.sql.Column;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.SqlFunction;
import org.symqle.sql.TableOrView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 21.11.12
 * Time: 20:50
 * To change this template use File | Settings | File Templates.
 */
public class CastSpecificationTest extends SqlTestCase {


    public void testShow() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C1 FROM person AS T1", col.show(new GenericDialect()));
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C1 FROM person AS T1", col.show(new GenericDialect()));
    }

    public void testCollate() throws Exception {
        final String sql = person.name.cast("CHAR(10)").collate("latin1_general_ci").show(new GenericDialect());
        assertSimilar("SELECT CAST(T1.name AS CHAR(10)) COLLATE latin1_general_ci AS C1 FROM person AS T1", sql);
    }

    public void testMap() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C1 FROM person AS T1", col.map(Mappers.LONG).show(new GenericDialect()));
    }

    public void testSelectStatementFunctionality() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0", col.show(new GenericDialect()));
    }

    public void testSelectAll() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT ALL CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0", col.all().show(new GenericDialect()));

    }

    public void testSelectDistinct() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT DISTINCT CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0", col.distinct().show(new GenericDialect()));
    }

    public void testOrderAsc() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0 ORDER BY C0 ASC", col.orderAsc().show(new GenericDialect()));
    }

    public void testOrderDesc() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T1.id AS NUMBER(12,0)) AS C0 FROM person AS T0 ORDER BY C0 DESC", col.orderDesc().show(new GenericDialect()));
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = SqlFunction.create("abs", Mappers.LONG).apply(person.id.cast("NUMBER (12,0)")).show(new GenericDialect());
        assertSimilar("SELECT abs(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testAsFunctionMultipleArguments() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = SqlFunction.create("max", Mappers.LONG).apply(column, column).show(new GenericDialect());
        assertSimilar("SELECT max(CAST(T1.id AS NUMBER(12,0)), CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = id.where(person.id.asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testEq() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(id.eq(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) = T0.age", sql);
    }

    public void testNe() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.ne(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.gt(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) > T0.age", sql);
    }

    public void testGe() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.ge(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.lt(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) < T0.age", sql);
    }

    public void testLe() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = person.id.where(column.le(age)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <= T0.age", sql);
    }

    public void testEqValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.eq(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.ne(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <> ?", sql);
    }

    public void testLtValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.lt(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.le(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) <= ?", sql);
    }

    public void testGtValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.gt(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER(12,0)");
        final String sql = person.id.where(id.ge(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) >= ?", sql);
    }

    public void testExceptAll() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.exceptAll(person2.age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.exceptDistinct(person.age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.except(person2.age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 EXCEPT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.unionAll(person2.age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 UNION ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.unionDistinct(person2.age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnion() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.union(person2.age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 UNION SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.intersectAll(person2.age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersectDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.intersectDistinct(person2.age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final String sql = column.intersect(person2.age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testUseSameTableInDistinct() throws Exception {
        final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
        final Column<Long> age = person.age;
        final String sql = column.intersectDistinct(age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testSelectForUpdate() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 FOR UPDATE", col.forUpdate().show(new GenericDialect()));
    }

    public void testSelectForReadOnly() throws Exception {
        final AbstractCastSpecification<Long> col = person.id.cast("NUMBER(12,0)");
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 FOR READ ONLY", col.forReadOnly().show(new GenericDialect()));
    }

    public void testExists() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final AbstractCastSpecification<Long> age2 = person2.age.cast("FLOAT");
        String sql = id.where(age2.exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT CAST(T1.age AS FLOAT) FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final AbstractCastSpecification<Long> age2 = person2.age.cast("FLOAT");
        String sql = id.where(age2.contains(20L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT CAST(T1.age AS FLOAT) FROM person AS T1)", sql);
    }

    public void testIn() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.cast("NUMBER (12,0)").in(id2)).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE CAST(T1.id AS NUMBER(12,0)) IN(SELECT T2.id FROM employee AS T2)", sql);
    }

    public void testInArgument() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.in(id2.cast("NUMBER (12,0)"))).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id IN(SELECT CAST(T2.id AS NUMBER(12,0)) FROM employee AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old
        final Column<Long> id2 = employee.id;
        String sql = id.where(id.cast("NUMBER (12,0)").notIn(id2)).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE CAST(T1.id AS NUMBER(12,0)) NOT IN(SELECT T2.id FROM employee AS T2)", sql);
    }

    public void testInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(id.cast("NUMBER (12,0)").in(1L, 2L, 3L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) IN(?, ?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final Column<Long> id  =  person.id;
        // find all but the most old

        String sql = id.where(id.cast("NUMBER (12,0)").notIn(1L, 2L, 3L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.id AS NUMBER(12,0)) NOT IN(?, ?, ?)", sql);
   }

    public void testIsNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(age.cast("NUMBER (12,0)").isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.age AS NUMBER(12,0)) IS NULL", sql);
   }

    public void testIsNotNull() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.where(age.cast("NUMBER (12,0)").isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.age AS NUMBER(12,0)) IS NOT NULL", sql);
   }

    public void testOrderBy() throws Exception {
        final Column<Long> age = person.age;
        String sql = person.id.cast("NUMBER (12,0)").orderBy(age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0 ORDER BY T0.age", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER (12,0)").nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER (12,0)").nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER (12,0)").desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER (12,0)").asc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0)) ASC", sql);
    }

    public void testSortSpecification() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.orderBy(age.cast("NUMBER (12,0)")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CAST(T0.age AS NUMBER(12,0))", sql);
    }

    public void testMult() throws Exception {
        final AbstractCastSpecification<Long> id = person.id.cast("NUMBER (12,0)");
        final Column<Long> age = person.age;
        String sql = id.mult(age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) * T0.age AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER (12,0)").pair(age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) AS C0, T0.age AS C1 FROM person AS T0", sql);
    }

    public void testFunction() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        SqlFunction<Long> sumOf = SqlFunction.create("SUM_OF", Mappers.LONG);
        String sql = sumOf.apply(id, age).show(new GenericDialect());
        assertSimilar("SELECT SUM_OF(T0.id, T0.age) AS C0 FROM person AS T0", sql);
    }

    public void testOpposite() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("NUMBER(12,0)").opposite().show(new GenericDialect());
        assertSimilar("SELECT - CAST(T0.id AS NUMBER(12,0)) AS C0 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER(12,0)").add(age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) + T0.age AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("NUMBER(12,0)").add(1).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) + ? AS C0 FROM person AS T0", sql);
    }

    public void testSub() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER(12,0)").sub(age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) - T0.age AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("NUMBER(12,0)").sub(1.0).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) - ? AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("NUMBER(12,0)").mult(2L).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) * ? AS C0 FROM person AS T0", sql);
    }

    public void testDiv() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER(12,0)").div(age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) / T0.age AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("NUMBER(12,0)").div(3).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS NUMBER(12,0)) / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final Column<Long> id  =  person.id;
        final Column<Long> age = person.age;
        String sql = id.cast("CHAR(12)").concat(age).show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS CHAR(12)) || T0.age AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final Column<Long> id  =  person.id;
        String sql = id.cast("CHAR(12)").concat(" (id)").show(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id AS CHAR(12)) || ? AS C0 FROM person AS T0", sql);
    }

    public void testCount() throws Exception {
        final String sql = person.id.cast("NUMBER(12,0)").count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.parentId.cast("NUMBER(12,0)").countDistinct().show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT CAST(T0.parent_id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.name.cast("CHAR(12)").notLike(DynamicParameter.create(Mappers.STRING, "John%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.name AS CHAR(12)) NOT LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.name.cast("CHAR(12)").notLike("John%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.name AS CHAR(12)) NOT LIKE ?", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(person.name.cast("CHAR(12)").like(DynamicParameter.create(Mappers.STRING, "John%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.name AS CHAR(12)) LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(person.name.cast("CHAR(12)").like("John%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.name AS CHAR(12)) LIKE ?", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.id.cast("NUMBER(12,0)").avg().show(new GenericDialect());
        assertSimilar("SELECT AVG(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.id.cast("NUMBER(12,0)").sum().show(new GenericDialect());
        assertSimilar("SELECT SUM(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.id.cast("NUMBER(12,0)").max().show(new GenericDialect());
        assertSimilar("SELECT MAX(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.id.cast("NUMBER(12,0)").min().show(new GenericDialect());
        assertSimilar("SELECT MIN(CAST(T0.id AS NUMBER(12,0))) AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(person.name.cast("BOOLEAN").asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CAST(T0.name AS BOOLEAN)", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.id.cast("NUMBER").cast("NUMBER(12,0)").show(new GenericDialect());
        assertSimilar("SELECT CAST(CAST(T0.id AS NUMBER) AS NUMBER(12,0)) AS C0 FROM person AS T0", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.id.cast("NUMBER").queryValue().where(employee.id.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT(SELECT CAST(T0.id AS NUMBER) FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.id IS NOT NULL", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.id).orElse(person.id.cast("NUMBER")).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.id ELSE CAST(T0.id AS NUMBER) END AS C0 FROM person AS T0", sql);
    }



    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final AbstractCastSpecification<Long> column, final DatabaseGate gate) throws SQLException {
                final List<Long> list = column.list(gate);
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final AbstractCastSpecification<Long> column, final DatabaseGate gate) throws SQLException {
                column.scroll(gate, new Callback<Long>() {
                    int rowNum = 0;
                    @Override
                    public boolean iterate(final Long value) {
                        assertEquals(1, ++rowNum);
                        assertEquals(123L, value.longValue());
                        return true;
                    }
                });
            }
        }.play();
    }

    public static abstract class Scenario {
        public void play() throws Exception {
            final AbstractCastSpecification<Long> column = person.id.cast("NUMBER(12,0)");
            final String queryString = column.show(new GenericDialect());
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(new GenericDialect());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection,  statement, resultSet);

            runQuery(column, gate);
            verify(gate, connection, statement, resultSet);

        }

        protected abstract void runQuery(final AbstractCastSpecification<Long> column, final DatabaseGate gate) throws SQLException;
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
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
