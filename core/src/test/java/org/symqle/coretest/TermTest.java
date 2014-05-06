package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractTerm;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Label;
import org.symqle.sql.TableOrView;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

/**
 * @author lvovich
 */
public class TermTest extends SqlTestCase {

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.id.mult(two).isNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.id.mult(two).isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? IS NOT NULL", sql);
    }


    public void testShow() throws Exception {
        final String sql = person.id.mult(two).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0", sql);
        assertSimilar(sql, person.id.mult(two).showQuery(new GenericDialect()));
    }

    public void testAdapt() throws Exception {
        final AbstractTerm<Long> adaptor = AbstractTerm.adapt(person.id);
        final String sql = adaptor.showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0", sql);
        assertEquals(adaptor.getMapper(), person.id.getMapper());
    }

    public void testMap() throws Exception {
        final AbstractTerm<Integer> remapped = person.id.mult(two).map(CoreMappers.INTEGER);
        final String sql = remapped.showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0", sql);
        assertSimilar(sql, person.id.mult(two).showQuery(new GenericDialect()));
        assertEquals(CoreMappers.INTEGER, remapped.getMapper());
    }

    public void testSelectAll() throws Exception {
        final String sql = person.id.mult(two).selectAll().showQuery(new GenericDialect());
        assertSimilar("SELECT ALL T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.id.mult(two).distinct().showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.mult(two).where(person.id.eq(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 WHERE T0.id = ?", sql);

    }

    public void testAsInValueList() throws Exception {
        final String sql = person.id.where(person.id.add(0).in(person.id.mult(two).asInValueList())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? IN(T0.id * ?)", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.id.mult(two).eq(person.id.add(0))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? = T0.id + ?", sql);
    }

    public void testEqArg() throws Exception {
        final String sql = person.id.where(person.id.add(0).eq(person.id.mult(two))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id + ? = T0.id * ?", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.id.mult(two).ne(person.id.add(0))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? <> T0.id + ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.id.mult(two).gt(person.id.add(0))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? > T0.id + ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.id.mult(two).ge(person.id.add(0))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? >= T0.id + ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.id.mult(two).lt(person.id.add(0))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? < T0.id + ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.id.mult(two).le(person.id.add(0))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? <= T0.id + ?", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.id.mult(two).eq(0)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.id.mult(two).ne(0)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.id.mult(two).gt(0)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.id.mult(two).ge(0)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.id.mult(two).lt(0)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.id.mult(two).le(0)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? <= ?", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.id.mult(two).in(person2.id.mult(two))).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id * ? IN(SELECT T2.id * ? FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.id.mult(two).notIn(person2.id.add(0))).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.id * ? NOT IN(SELECT T2.id + ? FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.id.mult(two).in(2L, 4L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.id.mult(two).notIn(2L, 4L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? NOT IN(?, ?)", sql);
   }

    public void testLabel() throws Exception {
        final Label l = new Label();
        String sql = person.id.mult(two).label(l).orderBy(l).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 ORDER BY C0", sql);
    }

    public void testOrderBy() throws Exception {
        String sql = person.id.mult(two).orderBy(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? AS C0 FROM person AS T0 ORDER BY T0.id * ?", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.id.mult(two).nullsFirst()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id * ? NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.id.mult(two).nullsLast()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id * ? NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.id.mult(two).desc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id * ? DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.id.mult(two).asc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id * ? ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = person.id.mult(two).opposite().showQuery(new GenericDialect());
        assertSimilar("SELECT -(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.id.mult(two).cast("NUMBER(10,2)").showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(T0.id * ? AS NUMBER(10,2)) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        String sql = person.id.mult(two).pair(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        String sql = person.id.mult(two).add(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? + T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        String sql = person.id.mult(two).add(2).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? + ? AS C0 FROM person AS T0", sql);
    }

    public void testParam() throws Exception {
        final AbstractTerm<Number> term = person.id.mult(two);
        String sql = term.add(term.param(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithoutValue() throws Exception {
        final AbstractTerm<Number> term = person.id.mult(two);
        String sql = term.add(term.param()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.id.mult(two).asBoolean()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.id * ?)", sql);
    }

    public void testSub() throws Exception {
        String sql = person.id.mult(two).sub(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? - T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        String sql = person.id.mult(two).sub(2).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.id.mult(two).mult(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? *(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.id.mult(two).mult(2).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? * ? AS C0 FROM person AS T0", sql);
    }


    public void testDiv() throws Exception {
        String sql = person.id.mult(two).div(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? /(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.id.mult(two).div(2).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id * ? / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        String sql = person.id.mult(two).concat(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.id * ?) ||(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        String sql = person.id.mult(two).concat(" ids").showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.id * ?) || ? AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        String sql = person.id.mult(two).substring(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.id * ?) FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCharLength() throws Exception {
        String sql = person.id.mult(two).charLength().showQuery(new GenericDialect());
        assertSimilar("SELECT CHAR_LENGTH((T0.id * ?)) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        String sql = person.id.mult(two).substring(person.id, person.id.div(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.id * ?) FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        String sql = person.id.mult(two).substring(2).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.id * ?) FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        String sql = person.id.mult(two).substring(2,5).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING((T0.id * ?) FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        String sql = person.id.mult(two).positionOf(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.id IN(T0.id * ?)) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        String sql = person.id.mult(two).positionOf("1").showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN(T0.id * ?)) AS C0 FROM person AS T0", sql);
    }





    public void testCollate() throws Exception {
        String sql = person.id.mult(two).collate("latin1_general_ci").showQuery(new GenericDialect());
        assertSimilar("SELECT(T0.id * ?) COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).union(person2.id.mult(1).where(person2.name.eq(employee.name))).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1 UNION SELECT T2.id * ? FROM person AS T2 WHERE T2.name = T0.name)", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).unionAll(person2.id.mult(1).where(person2.name.eq(employee.name))).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1 UNION ALL SELECT T2.id * ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).unionDistinct(person2.id.mult(1).where(person2.name.eq(employee.name))).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1 UNION DISTINCT SELECT T2.id * ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExcept() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).except(person2.id.mult(1).where(person2.name.eq(employee.name))).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1 EXCEPT SELECT T2.id * ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).exceptAll(person2.id.mult(1).where(person2.name.eq(employee.name))).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1 EXCEPT ALL SELECT T2.id * ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).exceptDistinct(person2.id.mult(1).where(person2.name.eq(employee.name))).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1 EXCEPT DISTINCT SELECT T2.id * ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }


    public void testIntersect() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).intersect(person2.id.mult(1).where(person2.name.eq(employee.name))).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1 INTERSECT SELECT T2.id * ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).intersectAll(person2.id.mult(1).where(person2.name.eq(employee.name))).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1 INTERSECT ALL SELECT T2.id * ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).intersectDistinct(person2.id.mult(1).where(person2.name.eq(employee.name))).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1 INTERSECT DISTINCT SELECT T2.id * ? FROM person AS T2 WHERE T2.name = t0.name)", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id * ? FROM person AS T1)", sql);
    }

    public void testCountRows() throws Exception {
        final String sql = person.id.mult(2).countRows().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(*) AS C0 FROM(SELECT T1.id * ? FROM person AS T1) AS T0", sql);
    }

    public void testContains() throws Exception {
        final String sql = employee.id.where(person.id.mult(2).contains(20)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT T1.id * ? FROM person AS T1)", sql);
    }

    public void testAll() throws Exception {
        final String sql = employee.id.where(employee.id.map(CoreMappers.NUMBER).lt(person.id.mult(2).all())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.id < ALL(SELECT T1.id * ? FROM person AS T1)", sql);
    }

    public void testAny() throws Exception {
        final String sql = employee.id.where(employee.id.map(CoreMappers.NUMBER).lt(person.id.mult(2).any())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.id < ANY(SELECT T1.id * ? FROM person AS T1)", sql);
    }

    public void testSome() throws Exception {
        final String sql = employee.id.where(employee.id.map(CoreMappers.NUMBER).lt(person.id.mult(2).some())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.id < SOME(SELECT T1.id * ? FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.mult(2).forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id * ? AS C1 FROM person AS T1 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.mult(2).forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id * ? AS C1 FROM person AS T1 FOR READ ONLY", sql);
    }

    public void testLimit() throws Exception {
        final String sql = person.id.mult(2).limit(20).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id * ? AS C1 FROM person AS T1 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = person.id.mult(2).limit(10, 20).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id * ? AS C1 FROM person AS T1 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.id.mult(2).queryValue().where(employee.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT T0.id * ? FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testWhenClause() throws Exception {
        final String sql = person.name.isNotNull().then(person.id.mult(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.id * ? END AS C0 FROM person AS T0", sql);
    }

    public void testElse() throws Exception {
        final String sql = person.name.isNotNull().then(person.id.add(1)).orElse(person.id.mult(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.id + ? ELSE T0.id * ? END AS C0 FROM person AS T0", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(person.id.mult(1).like(DynamicParameter.create(CoreMappers.STRING, "12%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.id.mult(1).notLike(DynamicParameter.create(CoreMappers.STRING, "12%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? NOT LIKE ?", sql);
    }

    public void  testLikeString() throws Exception {
        final String sql = person.id.where(person.id.mult(1).like("12%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? LIKE ?", sql);
    }
    
    public void  testNotLikeString() throws Exception {
        final String sql = person.id.where(person.id.mult(1).notLike("12%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id * ? NOT LIKE ?", sql);
    }

    public void testCount() throws Exception {
        final String sql = person.id.mult(1).count().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id * ?) AS C1 FROM person AS T1", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.id.mult(1).countDistinct().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT T1.id * ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testSum() throws Exception {
        final String sql = person.id.mult(1).sum().showQuery(new GenericDialect());
        assertSimilar("SELECT SUM(T1.id * ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testAvg() throws Exception {
        final String sql = person.id.mult(1).avg().showQuery(new GenericDialect());
        assertSimilar("SELECT AVG(T1.id * ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testMin() throws Exception {
        final String sql = person.id.mult(1).min().showQuery(new GenericDialect());
        assertSimilar("SELECT MIN(T1.id * ?) AS C1 FROM person AS T1", sql);
    }
    
    public void testMax() throws Exception {
        final String sql = person.id.mult(1).max().showQuery(new GenericDialect());
        assertSimilar("SELECT MAX(T1.id * ?) AS C1 FROM person AS T1", sql);
    }
    

    public void testListMult() throws Exception {
        new Scenario(person.id.mult(2)) {
            @Override
            void use(AbstractTerm<Number> query, QueryEngine engine) throws SQLException {
                final List<Number> list = query.list(engine);
                assertEquals(1, list.size());
                assertEquals(123, list.get(0).intValue());
            }
        }.play();
    }

    public void testListDiv() throws Exception {
        new Scenario(person.id.div(2)) {
            @Override
            void use(AbstractTerm<Number> query, QueryEngine engine) throws SQLException {
                final List<Number> list = query.list(engine);
                assertEquals(1, list.size());
                assertEquals(123, list.get(0).intValue());
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(person.id.mult(2)) {
            @Override
            void use(AbstractTerm<Number> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new NumberTestCallback()));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(person.id.mult(2)) {
            @Override
            void use(AbstractTerm<Number> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine).scroll(new NumberTestCallback()));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<Number, AbstractTerm<Number>> {

        private Scenario(AbstractTerm<Number> query) {
            super(query);
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            final OutBox param =createMock(OutBox.class);
            expect(parameters.next()).andReturn(param);
            param.setBigDecimal(new BigDecimal("2"));
            return Collections.singletonList(param);
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getBigDecimal()).andReturn(new BigDecimal("123"));
        }
    }
    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }
    
    private static DynamicParameter<Long> two = DynamicParameter.create(CoreMappers.LONG, 2L);

    private static Person person = new Person();
    private static Person person2 = new Person();
    
    private static class Employee extends TableOrView {
        @Override
        public String getTableName() {
            return "employee";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

    private static Employee employee = new Employee();
    


}
