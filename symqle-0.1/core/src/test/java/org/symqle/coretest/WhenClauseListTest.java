package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractSearchedWhenClauseList;
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
public class WhenClauseListTest extends SqlTestCase {

    private AbstractSearchedWhenClauseList<String> createWhenClauseList() {
        return person.age.gt(20L).then(person.name).orElse(person.nick);
    }

    public void testShow() throws Exception {
        final AbstractSearchedWhenClauseList<String> clauseList = createWhenClauseList();
        final String sql = clauseList.showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0", sql);
        assertSimilar(sql, clauseList.showQuery(new GenericDialect()));
    }

    public void testAdapt() throws Exception {
        final AbstractSearchedWhenClauseList<String> adaptor = AbstractSearchedWhenClauseList.adapt(person.age.gt(20L).then(person.name));
        final String sql = adaptor.showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0", sql);
        assertEquals(adaptor.getMapper(), person.name.getMapper());
    }

    public void testLimit() throws Exception {
        final String sql = createWhenClauseList().limit(20).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createWhenClauseList().limit(10, 20).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }


    public void testMap() throws Exception {
        final AbstractSearchedWhenClauseList<String> clauseList = createWhenClauseList();
        final String sql = clauseList.map(CoreMappers.STRING).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0", sql);
        assertSimilar(sql, clauseList.showQuery(new GenericDialect()));
    }

    public void testSelectAll() throws Exception {
        final String sql = createWhenClauseList().selectAll().showQuery(new GenericDialect());

        assertSimilar("SELECT ALL CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = createWhenClauseList().distinct().showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = createWhenClauseList().where(person.name.eq("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 WHERE T0.name = ?", sql);

    }

    public void testInValueList() throws Exception {
        final String sql = person.id.where(person.nick.in(createWhenClauseList().asInValueList())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.nick IN(CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END)", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(createWhenClauseList().eq(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END = T0.nick", sql);
    }

    public void testEqArg() throws Exception {
        final String sql = person.id.where(person.nick.eq(createWhenClauseList())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.nick = CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(createWhenClauseList().ne(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END <> T0.nick", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(createWhenClauseList().gt(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END > T0.nick", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(createWhenClauseList().ge(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END >= T0.nick", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(createWhenClauseList().lt(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END < T0.nick", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(createWhenClauseList().le(person.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END <= T0.nick", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(createWhenClauseList().eq("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(createWhenClauseList().ne("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(createWhenClauseList().gt("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(createWhenClauseList().ge("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(createWhenClauseList().lt("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(createWhenClauseList().le("John")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END <= ?", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(createWhenClauseList().in(person2.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END IN(SELECT T1.nick FROM person AS T1)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(createWhenClauseList().notIn(person2.nick)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END NOT IN(SELECT T1.nick FROM person AS T1)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(createWhenClauseList().in("John", "James")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(createWhenClauseList().notIn("John", "James")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END NOT IN(?, ?)", sql);
   }

    public void testAsInSubquery() throws Exception {
        final String sql = person2.id.where(person2.nick.in(createWhenClauseList())).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.nick IN(SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END FROM person AS T0)", sql);
    }

    public void testAsElseArgument() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orElse(person.age.lt(20L).then(person.nick).orElse(person.name)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE CASE WHEN T0.age < ? THEN T0.nick ELSE T0.name END END AS C0 FROM person AS T0", sql);
    }

    public void testSort() throws Exception {
        String sql = person.id.orderBy(createWhenClauseList()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END", sql);
    }


    public void testLabel() throws Exception {
        final Label l = new Label();
        final String sql = createWhenClauseList().label(l).orderBy(l).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 ORDER BY C0", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = createWhenClauseList().orderBy(person.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 ORDER BY T0.nick", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(createWhenClauseList().nullsFirst()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(createWhenClauseList().nullsLast()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(createWhenClauseList().desc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(createWhenClauseList().asc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = person.name.eq("John").then(person.age).orElse(person.id).opposite().showQuery(new GenericDialect());
        assertSimilar("SELECT - CASE WHEN T0.name = ? THEN T0.age ELSE T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.name.eq("John").then(person.age).orElse(person.id).cast("NUMBER(10,2)").showQuery(new GenericDialect());
        assertSimilar("SELECT CAST(CASE WHEN T0.name = ? THEN T0.age ELSE T0.id END AS NUMBER(10,2)) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        String sql = person.age.gt(20L).then(person.nick).orElse(person.name).pair(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.nick ELSE T0.name END AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orElse(person.age).add(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END + T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orElse(person.age).add(2).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithValue() throws Exception {
        final AbstractSearchedWhenClauseList<Long> whenClauseList = person.age.gt(20L).then(person.id).orElse(person.age);
        String sql = whenClauseList.add(whenClauseList.param(2L)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithoutValue() throws Exception {
        final AbstractSearchedWhenClauseList<Long> whenClauseList = person.age.gt(20L).then(person.id).orElse(person.age);
        String sql = whenClauseList.add(whenClauseList.param()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.age.gt(20L).then(person.id).orElse(person.age).asBoolean()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END", sql);
    }

    public void testSub() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orElse(person.age).sub(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END - T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orElse(person.age).sub(2).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orElse(person.age).mult(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END *(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orElse(person.age).mult(2).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END * ? AS C0 FROM person AS T0", sql);
    }


    public void testDiv() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orElse(person.age).div(person.id.mult(two)).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END /(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orElse(person.age).div(2).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final String sql = createWhenClauseList().concat(person.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END || T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = createWhenClauseList().concat(" test").showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END || ? AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = createWhenClauseList().substring(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCharLength() throws Exception {
        final String sql = createWhenClauseList().charLength().showQuery(new GenericDialect());
        assertSimilar("SELECT CHAR_LENGTH(CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = createWhenClauseList().substring(person.id, person.id.div(2)).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = createWhenClauseList().substring(2).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = createWhenClauseList().substring(2, 5).showQuery(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        final String sql = createWhenClauseList().positionOf(person.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.nick IN CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        final String sql = createWhenClauseList().positionOf("A").showQuery(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END) AS C0 FROM person AS T0", sql);
    }


    public void testCollate() throws Exception {
        final String sql = createWhenClauseList().collate("latin1_general_ci").showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = createWhenClauseList().union(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 UNION SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = createWhenClauseList().unionAll(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 UNION ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = createWhenClauseList().unionDistinct(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = createWhenClauseList().except(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 EXCEPT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = createWhenClauseList().exceptAll(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = createWhenClauseList().exceptDistinct(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }


    public void testIntersect() throws Exception {
        final String sql = createWhenClauseList().intersect(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 INTERSECT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = createWhenClauseList().intersectAll(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = createWhenClauseList().intersectDistinct(person2.nick).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(createWhenClauseList().exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT CASE WHEN T1.age > ? THEN T1.name ELSE T1.nick END FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = employee.id.where(createWhenClauseList().contains("Jim")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT CASE WHEN T1.age > ? THEN T1.name ELSE T1.nick END FROM person AS T1)", sql);
    }

    public void testAll() throws Exception {
        final String sql = employee.id.where(employee.name.lt(createWhenClauseList().all())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.name < ALL(SELECT CASE WHEN T1.age > ? THEN T1.name ELSE T1.nick END FROM person AS T1)", sql);
    }

    public void testAny() throws Exception {
        final String sql = employee.id.where(employee.name.lt(createWhenClauseList().any())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.name < ANY(SELECT CASE WHEN T1.age > ? THEN T1.name ELSE T1.nick END FROM person AS T1)", sql);
    }

    public void testSome() throws Exception {
        final String sql = employee.id.where(employee.name.lt(createWhenClauseList().some())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE T0.name < SOME(SELECT CASE WHEN T1.age > ? THEN T1.name ELSE T1.nick END FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = createWhenClauseList().forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createWhenClauseList().forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = createWhenClauseList().queryValue().where(employee.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }
    
    public void testIsNull() throws Exception {
        final String sql = person.id.where(createWhenClauseList().isNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(createWhenClauseList().isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END IS NOT NULL", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(createWhenClauseList().like(DynamicParameter.create(CoreMappers.STRING, "J%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(createWhenClauseList().notLike(DynamicParameter.create(CoreMappers.STRING, "J%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(createWhenClauseList().like("J%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(createWhenClauseList().notLike("J%")).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END NOT LIKE ?", sql);
    }

    public void testCount() throws Exception {
        final String sql = person.age.gt(20L).then(person.id).orElse(person.age).count().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END) AS C0 FROM person AS T0", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.id).orElse(person.age).countDistinct().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT CASE WHEN T1.age > ? THEN T1.id ELSE T1.age END) AS C1 FROM person AS T1", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.age.gt(20L).then(person.id).orElse(person.age).avg().showQuery(new GenericDialect());
        assertSimilar("SELECT AVG(CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END) AS C0 FROM person AS T0", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.age.gt(20L).then(person.id).orElse(person.age).sum().showQuery(new GenericDialect());
        assertSimilar("SELECT SUM(CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END) AS C0 FROM person AS T0", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.age.gt(20L).then(person.id).orElse(person.age).min().showQuery(new GenericDialect());
        assertSimilar("SELECT MIN(CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END) AS C0 FROM person AS T0", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.age.gt(20L).then(person.id).orElse(person.age).max().showQuery(new GenericDialect());
        assertSimilar("SELECT MAX(CASE WHEN T0.age > ? THEN T0.id ELSE T0.age END) AS C0 FROM person AS T0", sql);
    }



    public void testList() throws Exception {
        new Scenario(createWhenClauseList()) {
            @Override
            void use(AbstractSearchedWhenClauseList<String> query, QueryEngine engine) throws SQLException {
                assertEquals(Arrays.asList("John"), query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(createWhenClauseList()) {
            @Override
            void use(AbstractSearchedWhenClauseList<String> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<String>("John")));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(createWhenClauseList()) {
            @Override
            void use(AbstractSearchedWhenClauseList<String> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine).scroll(new TestCallback<String>("John")));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<String, AbstractSearchedWhenClauseList<String>> {
        protected Scenario(AbstractSearchedWhenClauseList<String> query) {
            super(query);
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            final OutBox param1 =createMock(OutBox.class);
            final OutBox param2 =createMock(OutBox.class);
            expect(parameters.next()).andReturn(param1);
            param1.setLong(20L);
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
