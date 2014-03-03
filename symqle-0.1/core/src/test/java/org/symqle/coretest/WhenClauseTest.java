package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractSearchedWhenClause;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Label;
import org.symqle.sql.SqlFunction;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

/**
 * @author lvovich
 */
public class WhenClauseTest extends SqlTestCase {

    public void testSelect() throws Exception {
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause();
        final String sql = whenClause.show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0", sql);
        assertSimilar(sql, whenClause.show(new GenericDialect()));
    }

    private AbstractSearchedWhenClause<String> createWhenClause() {
        return person.age.gt(20L).then(person.name);
    }

    public void testMap() throws Exception {
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause();
        final String sql = whenClause.map(CoreMappers.STRING).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0", sql);
    }
    public void testElse() throws Exception {
        final String sql = createWhenClause().orElse(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testDynamicParameterElse() throws Exception {
        final String sql = createWhenClause().orElse(DynamicParameter.create(CoreMappers.STRING, "do not care")).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE ? END AS C0 FROM person AS T0", sql);
    }

    public void testFunctionElse() throws Exception {
        final String sql = createWhenClause().orElse(SqlFunction.create("to_upper", CoreMappers.STRING).apply(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE to_upper(T0.nick) END AS C0 FROM person AS T0", sql);
    }

    public void testChain() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orWhen(person.age.gt(20L).then(person.name.concat(" Jr."))).orElse(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.name || ? ELSE T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orWhen(person.age.gt(20L).thenNull()).orElse(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN NULL ELSE T0.nick END AS C0 FROM person AS T0", sql);
    }


    public void testSelectAll() throws Exception {
        final String sql = createWhenClause().selectAll().show(new GenericDialect());
        assertSimilar("SELECT ALL CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = createWhenClause().distinct().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0", sql);
    }

    public void testLimit() throws Exception {
        final String sql = createWhenClause().limit(20).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createWhenClause().limit(10, 20).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testAdapt() throws Exception {
        final AbstractSearchedWhenClause<String> whenClause = createWhenClause();
        final AbstractSearchedWhenClause<String> adaptor = AbstractSearchedWhenClause.adapt(whenClause);
        assertEquals(whenClause.show(new GenericDialect()), adaptor.show(new GenericDialect()));
        assertEquals(whenClause.getMapper(), adaptor.getMapper());
    }

    public void testWhere() throws Exception {
        final String sql = createWhenClause().where(person.name.eq("John")).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 WHERE T0.name = ?", sql);

    }

    public void testAsInValueList() throws Exception {
        final String sql = person.id.where(person.nick.in(createWhenClause().asInValueList())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.nick IN(CASE WHEN T0.age > ? THEN T0.name END)", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(createWhenClause().eq(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END = T0.nick", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(createWhenClause().ne(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END <> T0.nick", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(createWhenClause().gt(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END > T0.nick", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(createWhenClause().ge(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END >= T0.nick", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(createWhenClause().lt(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END < T0.nick", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(createWhenClause().le(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END <= T0.nick", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(createWhenClause().eq("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(createWhenClause().ne("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(createWhenClause().gt("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(createWhenClause().ge("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(createWhenClause().lt("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(createWhenClause().le("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END <= ?", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(createWhenClause().in(person2.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END IN(SELECT T1.nick FROM person AS T1)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(createWhenClause().notIn(person2.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END NOT IN(SELECT T1.nick FROM person AS T1)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(createWhenClause().in("John", "James")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(createWhenClause().notIn("John", "James")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END NOT IN(?, ?)", sql);
   }

    public void testAsInSubquery() throws Exception {
        final String sql = person2.id.where(person2.nick.in(createWhenClause())).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.nick IN(SELECT CASE WHEN T0.age > ? THEN T0.name END FROM person AS T0)", sql);
    }

    public void testContains() throws Exception {
        final String sql = person2.id.where(createWhenClause().contains("Jim")).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE ? IN(SELECT CASE WHEN T0.age > ? THEN T0.name END FROM person AS T0)", sql);
    }

    public void testAsElseArgument() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orElse(person.age.lt(20L).then(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE CASE WHEN T0.age < ? THEN T0.nick END END AS C0 FROM person AS T0", sql);
    }

    public void testSort() throws Exception {
        String sql = person.id.orderBy(createWhenClause()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name END", sql);
    }

    public void testLabel() throws Exception {
        final Label l = new Label();
        final String sql = createWhenClause().label(l).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 ORDER BY C0", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = createWhenClause().orderBy(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 ORDER BY T0.nick", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(createWhenClause().nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name END NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(createWhenClause().nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name END NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(createWhenClause().desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name END DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(createWhenClause().asc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name END ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = person.name.eq("John").then(person.age).opposite().show(new GenericDialect());
        assertSimilar("SELECT - CASE WHEN T0.name = ? THEN T0.age END AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.name.eq("John").then(person.age).cast("CHAR(10)").show(new GenericDialect());
        assertSimilar("SELECT CAST(CASE WHEN T0.name = ? THEN T0.age END AS CHAR(10)) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        String sql = person.age.gt(20L).then(person.nick).pair(person.name).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.nick END AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        String sql = person.age.gt(20L).then(person.id).add(person.id.mult(two)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id END + T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).add(2).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id END + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithValue() throws Exception {
        final AbstractSearchedWhenClause<Long> searchedWhenClause = person.age.gt(20L).then(person.id);
        String sql = searchedWhenClause.add(searchedWhenClause.param(2L)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id END + ? AS C0 FROM person AS T0", sql);
    }

    public void testParamWithNoValue() throws Exception {
        final AbstractSearchedWhenClause<Long> searchedWhenClause = person.age.gt(20L).then(person.id);
        String sql = searchedWhenClause.add(searchedWhenClause.param()).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id END + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.age.gt(20L).then(person.id).asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.id END", sql);
    }

    public void testSub() throws Exception {
        String sql = person.age.gt(20L).then(person.id).sub(person.id.mult(two)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id END - T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).sub(2).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id END - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.age.gt(20L).then(person.id).mult(person.id.mult(two)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id END *(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).mult(2).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id END * ? AS C0 FROM person AS T0", sql);
    }


    public void testDiv() throws Exception {
        String sql = person.age.gt(20L).then(person.id).div(person.id.mult(two)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id END /(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).div(2).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id END / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final String sql = createWhenClause().concat(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END || T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = createWhenClause().concat(" test").show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END || ? AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = createWhenClause().substring(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name END FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = createWhenClause().substring(person.id, person.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name END FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = createWhenClause().substring(2).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name END FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = createWhenClause().substring(2, 5).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name END FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        final String sql = createWhenClause().positionOf(person.nick).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.nick IN CASE WHEN T0.age > ? THEN T0.name END) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        final String sql = createWhenClause().positionOf("A").show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN CASE WHEN T0.age > ? THEN T0.name END) AS C0 FROM person AS T0", sql);
    }



    public void testCollate() throws Exception {
        final String sql = createWhenClause().collate("latin1_general_ci").show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = createWhenClause().union(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 UNION SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = createWhenClause().unionAll(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 UNION ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = createWhenClause().unionDistinct(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = createWhenClause().except(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 EXCEPT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = createWhenClause().exceptAll(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = createWhenClause().exceptDistinct(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }


    public void testIntersect() throws Exception {
        final String sql = createWhenClause().intersect(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 INTERSECT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = createWhenClause().intersectAll(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = createWhenClause().intersectDistinct(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(createWhenClause().exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT CASE WHEN T1.age > ? THEN T1.name END FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = createWhenClause().forUpdate().show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createWhenClause().forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = createWhenClause().queryValue().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT(SELECT CASE WHEN T0.age > ? THEN T0.name END FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }
    
    public void testIsNull() throws Exception {
        final String sql = person.id.where(createWhenClause().isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(createWhenClause().isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END IS NOT NULL", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(createWhenClause().like(DynamicParameter.create(CoreMappers.STRING, "J%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(createWhenClause().notLike(DynamicParameter.create(CoreMappers.STRING, "J%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(createWhenClause().like("J%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(createWhenClause().notLike("J%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END NOT LIKE ?", sql);
    }

    public void testCount() throws Exception {
        final String sql = person.age.gt(20L).then(person.age).count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(CASE WHEN T0.age > ? THEN T0.age END) AS C0 FROM person AS T0", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.age).countDistinct().show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT CASE WHEN T0.age > ? THEN T0.age END) AS C0 FROM person AS T0", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.age.gt(20L).then(person.age).avg().show(new GenericDialect());
        assertSimilar("SELECT AVG(CASE WHEN T0.age > ? THEN T0.age END) AS C0 FROM person AS T0", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.age.gt(20L).then(person.age).sum().show(new GenericDialect());
        assertSimilar("SELECT SUM(CASE WHEN T0.age > ? THEN T0.age END) AS C0 FROM person AS T0", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.age.gt(20L).then(person.age).min().show(new GenericDialect());
        assertSimilar("SELECT MIN(CASE WHEN T0.age > ? THEN T0.age END) AS C0 FROM person AS T0", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.age.gt(20L).then(person.age).max().show(new GenericDialect());
        assertSimilar("SELECT MAX(CASE WHEN T0.age > ? THEN T0.age END) AS C0 FROM person AS T0", sql);
    }

    public void testList() throws Exception {
        new Scenario(createWhenClause()) {
            @Override
            void use(AbstractSearchedWhenClause<String> query, QueryEngine engine) throws SQLException {
                assertEquals(Arrays.asList("John"), query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(createWhenClause()) {
            @Override
            void use(AbstractSearchedWhenClause<String> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<String>("John")));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(createWhenClause()) {
            @Override
            void use(AbstractSearchedWhenClause<String> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine).scroll(new TestCallback<String>("John")));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<String, AbstractSearchedWhenClause<String>> {
        protected Scenario(AbstractSearchedWhenClause<String> query) {
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
