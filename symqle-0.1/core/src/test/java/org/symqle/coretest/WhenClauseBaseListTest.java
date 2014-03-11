package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractSearchedWhenClause;
import org.symqle.sql.AbstractSearchedWhenClauseBaseList;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Label;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

/**
 * @author lvovich
 */
public class WhenClauseBaseListTest extends SqlTestCase {

    private AbstractSearchedWhenClauseBaseList<String> createWhenClauseBaseList() {
        return person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick));
    }

    public void testShow() throws Exception {
        final AbstractSearchedWhenClauseBaseList<String> baseList = createWhenClauseBaseList();
        final String sql = baseList.showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0", sql);
        assertSimilar(sql, baseList.showQuery(new GenericDialect()));
    }

    public void testAdapt() throws Exception {
        final AbstractSearchedWhenClause<String> adaptee = person.age.gt(20L).then(person.name);
        final AbstractSearchedWhenClauseBaseList<String> adaptor = AbstractSearchedWhenClauseBaseList.adapt(adaptee);
        final String sql = adaptor.showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0", sql);
        assertEquals(adaptee.getMapper(), adaptor.getMapper());
    }

    public void testLimit() throws Exception {
        final String sql = createWhenClauseBaseList().limit(20).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createWhenClauseBaseList().limit(10, 20).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testMap() throws Exception {
        final AbstractSearchedWhenClauseBaseList<String> baseList = createWhenClauseBaseList();
        final String sql = baseList.map(CoreMappers.STRING).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0", sql);
        assertSimilar(sql, baseList.showQuery(new GenericDialect()));
    }

    public void testElse() throws Exception {
        final String sql = createWhenClauseBaseList().orElse(person.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick ELSE T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testChain() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orWhen(person.age.gt(20L).then(person.name.concat(" Jr."))).orWhen(person.age.gt(1L).then(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.name || ? WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).orWhen(person.age.gt(20L).thenNull()).orElse(person.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick WHEN T0.age > ? THEN NULL ELSE T0.nick END AS C0 FROM person AS T0", sql);
    }


    public void testSelectAll() throws Exception {
        final String sql = createWhenClauseBaseList().selectAll().showQuery(new GenericDialect());
        assertSimilar("SELECT ALL CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = createWhenClauseBaseList().distinct().showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = createWhenClauseBaseList().where(person.name.eq("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 WHERE T0.name = ?", sql);

    }

    public void testInValueList() throws Exception {
        final String sql = person.id.where(person.nick.in(createWhenClauseBaseList().asInValueList())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.nick IN(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END)", sql);
    }


    public void testEq() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().eq(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END = T0.nick", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().ne(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END <> T0.nick", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().gt(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END > T0.nick", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().ge(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END >= T0.nick", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().lt(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END < T0.nick", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().le(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END <= T0.nick", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().eq("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().ne("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().gt("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().ge("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().lt("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().le("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END <= ?", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().in(person2.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END IN(SELECT T1.nick FROM person AS T1)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().notIn(person2.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NOT IN(SELECT T1.nick FROM person AS T1)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().in("John", "James")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().notIn("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NOT IN(?)", sql);
   }

    public void testAsInSubquery() throws Exception {
        final String sql = person2.id.where(person2.nick.in(createWhenClauseBaseList())).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.nick IN(SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM person AS T0)", sql);
    }

    public void testAsElseArgument() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orElse(person.age.lt(20L).then(person.nick).orWhen(person.age.gt(1L).then(person.name))).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE CASE WHEN T0.age < ? THEN T0.nick WHEN T0.age > ? THEN T0.name END END AS C0 FROM person AS T0", sql);
    }

    public void testSort() throws Exception {
        String sql = person.id.orderBy(createWhenClauseBaseList()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END", sql);
    }

    public void testLabel() throws Exception {
        final Label l = new Label();
        final String sql = createWhenClauseBaseList().label(l).orderBy(l).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 ORDER BY C0", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = createWhenClauseBaseList().orderBy(person.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 ORDER BY T0.nick", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(createWhenClauseBaseList().nullsFirst()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(createWhenClauseBaseList().nullsLast()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(createWhenClauseBaseList().desc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(createWhenClauseBaseList().asc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = person.name.eq("John").then(person.age).orWhen(person.age.gt(1L).then(person.id)).opposite().showQuery(new GenericDialect());
        assertSimilar("SELECT - CASE WHEN T0.name = ? THEN T0.age WHEN T0.age > ? THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.name.eq("John").then(person.age).orWhen(person.age.gt(1L).then(person.id)).cast("NUMBER(12,2)").showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(CASE WHEN T0.name = ? THEN T0.age WHEN T0.age > ? THEN T0.id END AS NUMBER(12,2)) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        String sql = createWhenClauseBaseList().pair(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).add(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END + T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).add(2L).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithValue() throws Exception {
        final AbstractSearchedWhenClauseBaseList<Long> whenClauseBaseList = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age));
        String sql = whenClauseBaseList.add(whenClauseBaseList.param(2L)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithoutValue() throws Exception {
        final AbstractSearchedWhenClauseBaseList<Long> whenClauseBaseList = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age));
        String sql = whenClauseBaseList.add(whenClauseBaseList.param()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).asPredicate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END", sql);
    }

    public void testSub() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).sub(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END - T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).sub(2L).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).mult(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END *(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).mult(2L).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END * ? AS C0 FROM person AS T0", sql);
    }


    public void testDiv() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).div(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END /(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).div(2L).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final String sql = createWhenClauseBaseList().concat(person.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END || T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = createWhenClauseBaseList().concat(" test").showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END || ? AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = createWhenClauseBaseList().substring(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = createWhenClauseBaseList().substring(person.id, person.id.div(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = createWhenClauseBaseList().substring(2).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = createWhenClauseBaseList().substring(2, 5).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        final String sql = createWhenClauseBaseList().positionOf(person.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.nick IN CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        final String sql = createWhenClauseBaseList().positionOf("A").showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }


    public void testCollate() throws Exception {
        final String sql = createWhenClauseBaseList().collate("latin1_general_ci").showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = createWhenClauseBaseList().union(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 UNION SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = createWhenClauseBaseList().unionAll(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 UNION ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = createWhenClauseBaseList().unionDistinct(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = createWhenClauseBaseList().except(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 EXCEPT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = createWhenClauseBaseList().exceptAll(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = createWhenClauseBaseList().exceptDistinct(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }


    public void testIntersect() throws Exception {
        final String sql = createWhenClauseBaseList().intersect(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 INTERSECT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = createWhenClauseBaseList().intersectAll(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = createWhenClauseBaseList().intersectDistinct(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(createWhenClauseBaseList().exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT CASE WHEN T1.age > ? THEN T1.name WHEN T0.age > ? THEN T0.nick END FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = employee.id.where(createWhenClauseBaseList().contains("Jim")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT CASE WHEN T1.age > ? THEN T1.name WHEN T0.age > ? THEN T0.nick END FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = createWhenClauseBaseList().forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createWhenClauseBaseList().forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = createWhenClauseBaseList().queryValue().where(employee.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }
    
    public void testIsNull() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().isNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END IS NOT NULL", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().like(DynamicParameter.create(CoreMappers.STRING, "J%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().notLike(DynamicParameter.create(CoreMappers.STRING, "J%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().like("J%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(createWhenClauseBaseList().notLike("J%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NOT LIKE ?", sql);
    }

    public void testCount() throws Exception {
        final String sql = createWhenClauseBaseList().count().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = createWhenClauseBaseList().countDistinct().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testAvg() throws Exception {
        final String sql = createWhenClauseBaseList().avg().showQuery(new GenericDialect());
        assertSimilar("SELECT AVG(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testSum() throws Exception {
        final String sql = createWhenClauseBaseList().sum().showQuery(new GenericDialect());
        assertSimilar("SELECT SUM(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testMin() throws Exception {
        final String sql = createWhenClauseBaseList().min().showQuery(new GenericDialect());
        assertSimilar("SELECT MIN(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testMax() throws Exception {
        final String sql = createWhenClauseBaseList().max().showQuery(new GenericDialect());
        assertSimilar("SELECT MAX(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }



    public void testList() throws Exception {
        new Scenario(createWhenClauseBaseList()) {
            @Override
            void use(AbstractSearchedWhenClauseBaseList<String> query, QueryEngine engine) throws SQLException {
                assertEquals(Arrays.asList("John"), query.list(engine));
            }
        }.play();
    }
    public void testScroll() throws Exception {
        new Scenario(createWhenClauseBaseList()) {
            @Override
            void use(AbstractSearchedWhenClauseBaseList<String> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<String>("John")));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(createWhenClauseBaseList()) {
            @Override
            void use(AbstractSearchedWhenClauseBaseList<String> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine).scroll(new TestCallback<String>("John")));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<String, AbstractSearchedWhenClauseBaseList<String>> {
        protected Scenario(AbstractSearchedWhenClauseBaseList<String> query) {
            super(query);
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            final OutBox param1 =createMock(OutBox.class);
            final OutBox param2 =createMock(OutBox.class);
            expect(parameters.next()).andReturn(param1);
            param1.setLong(20L);
            expect(parameters.next()).andReturn(param2);
            param2.setLong(1L);
            return Arrays.asList(param1, param2);
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getString()).andReturn("John");
        }
    }

    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<String> nick = defineColumn(CoreMappers.STRING, "nick");
        public Column<Long> age = defineColumn(CoreMappers.LONG, "age");
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
