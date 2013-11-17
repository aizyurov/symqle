package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameter;
import org.symqle.common.SqlParameters;
import org.symqle.sql.AbstractSearchedWhenClauseBaseList;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class WhenClauseBaseListTest extends SqlTestCase {

    public void testShow() throws Exception {
        final AbstractSearchedWhenClauseBaseList<String> baseList = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick));
        final String sql = baseList.show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0", sql);
        assertSimilar(sql, baseList.show(new GenericDialect()));
    }

    public void testMap() throws Exception {
        final AbstractSearchedWhenClauseBaseList<String> baseList = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick));
        final String sql = baseList.map(Mappers.STRING).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0", sql);
        assertSimilar(sql, baseList.show(new GenericDialect()));
    }

    public void testElse() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).orElse(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick ELSE T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testChain() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orWhen(person.age.gt(20L).then(person.name.concat(" Jr."))).orWhen(person.age.gt(1L).then(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.name || ? WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).orWhen(person.age.gt(20L).thenNull()).orElse(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick WHEN T0.age > ? THEN NULL ELSE T0.nick END AS C0 FROM person AS T0", sql);
    }


    public void testSelectAll() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).selectAll().show(new GenericDialect());
        assertSimilar("SELECT ALL CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).distinct().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).where(person.name.eq("John")).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 WHERE T0.name = ?", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).eq(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END = T0.nick", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).ne(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END <> T0.nick", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).gt(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END > T0.nick", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).ge(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END >= T0.nick", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).lt(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END < T0.nick", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).le(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END <= T0.nick", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).eq("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).ne("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).gt("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).ge("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).lt("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).le("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END <= ?", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).in(person2.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END IN(SELECT T1.nick FROM person AS T1)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).notIn(person2.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NOT IN(SELECT T1.nick FROM person AS T1)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).in("John", "James")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).notIn("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NOT IN(?)", sql);
   }

    public void testAsInSubquery() throws Exception {
        final String sql = person2.id.where(person2.nick.in(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)))).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.nick IN(SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM person AS T0)", sql);
    }

    public void testAsElseArgument() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orElse(person.age.lt(20L).then(person.nick).orWhen(person.age.gt(1L).then(person.name))).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE CASE WHEN T0.age < ? THEN T0.nick WHEN T0.age > ? THEN T0.name END END AS C0 FROM person AS T0", sql);
    }

    public void testSort() throws Exception {
        String sql = person.id.orderBy(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).orderBy(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 ORDER BY T0.nick", sql);
    }

    public void testOrderAsc() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).orderAsc().show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 ORDER BY C0 ASC", sql);
    }

    public void testOrderDesc() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).orderDesc().show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 ORDER BY C0 DESC", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).asc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END ASC", sql);
    }


    public void testOpposite() throws Exception {
        final String sql = person.name.eq("John").then(person.age).orWhen(person.age.gt(1L).then(person.id)).opposite().show(new GenericDialect());
        assertSimilar("SELECT - CASE WHEN T0.name = ? THEN T0.age WHEN T0.age > ? THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testCast() throws Exception {
        final String sql = person.name.eq("John").then(person.age).orWhen(person.age.gt(1L).then(person.id)).cast("NUMBER(12,2)").show(new GenericDialect());
        assertSimilar("SELECT CAST(CASE WHEN T0.name = ? THEN T0.age WHEN T0.age > ? THEN T0.id END AS NUMBER(12,2)) AS C0 FROM person AS T0", sql);
    }

    public void testPair() throws Exception {
        String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).pair(person.name).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testAdd() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).add(person.id.mult(two)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END + T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testAddNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).add(2L).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END + ? AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        String sql = person.id.where(person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END", sql);
    }

    public void testSub() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).sub(person.id.mult(two)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END - T0.id * ? AS C0 FROM person AS T0", sql);
    }

    public void testSubNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).sub(2L).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END - ? AS C0 FROM person AS T0", sql);
    }

    public void testMult() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).mult(person.id.mult(two)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END *(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testMultNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).mult(2L).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END * ? AS C0 FROM person AS T0", sql);
    }


    public void testDiv() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).div(person.id.mult(two)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END /(T0.id * ?) AS C0 FROM person AS T0", sql);
    }

    public void testDivNumber() throws Exception {
        String sql = person.age.gt(20L).then(person.id).orWhen(person.age.gt(1L).then(person.age)).div(2L).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.id WHEN T0.age > ? THEN T0.age END / ? AS C0 FROM person AS T0", sql);
    }

    public void testConcat() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).concat(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END || T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).concat(" test").show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END || ? AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).substring(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).substring(person.id, person.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).substring(2).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).substring(2, 5).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).positionOf(person.nick).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.nick IN CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).positionOf("A").show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }


    public void testCollate() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).collate("latin1_general_ci").show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).union(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 UNION SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).unionAll(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 UNION ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).unionDistinct(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).except(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 EXCEPT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).exceptAll(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).exceptDistinct(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }


    public void testIntersect() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).intersect(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 INTERSECT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).intersectAll(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).intersectDistinct(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT CASE WHEN T1.age > ? THEN T1.name WHEN T0.age > ? THEN T0.nick END FROM person AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = employee.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).contains("Jim")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT CASE WHEN T1.age > ? THEN T1.name WHEN T0.age > ? THEN T0.nick END FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).queryValue().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT(SELECT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }
    
    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END IS NOT NULL", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).like(DynamicParameter.create(Mappers.STRING, "J%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).notLike(DynamicParameter.create(Mappers.STRING, "J%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).like("J%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).notLike("J%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END NOT LIKE ?", sql);
    }

    public void testCount() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testCountDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).countDistinct().show(new GenericDialect());
        assertSimilar("SELECT COUNT(DISTINCT CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testAvg() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).avg().show(new GenericDialect());
        assertSimilar("SELECT AVG(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testSum() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).sum().show(new GenericDialect());
        assertSimilar("SELECT SUM(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testMin() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).min().show(new GenericDialect());
        assertSimilar("SELECT MIN(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }

    public void testMax() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick)).max().show(new GenericDialect());
        assertSimilar("SELECT MAX(CASE WHEN T0.age > ? THEN T0.name WHEN T0.age > ? THEN T0.nick END) AS C0 FROM person AS T0", sql);
    }



    public void testList() throws Exception {
        final AbstractSearchedWhenClauseBaseList<String> whenClause = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick));
        final String queryString = whenClause.show(new GenericDialect());
        final List<String> expected = Arrays.asList("John");
        final SqlParameters parameters = createMock(SqlParameters.class);
        final SqlParameter param1 =createMock(SqlParameter.class);
        final SqlParameter param2 =createMock(SqlParameter.class);
        expect(parameters.next()).andReturn(param1);
        expect(parameters.next()).andReturn(param2);
        param1.setLong(20L);
        param2.setLong(1L);
        replay(parameters, param1, param2);
        final List<String> list = whenClause.list(new MockQueryEngine<String>(new SqlContext(),
                expected, queryString, parameters));
        assertEquals(expected, list);
        verify(parameters, param1, param2);

    }
    public void testScroll() throws Exception {
        final AbstractSearchedWhenClauseBaseList<String> whenClause = person.age.gt(20L).then(person.name).orWhen(person.age.gt(1L).then(person.nick));
        final String queryString = whenClause.show(new GenericDialect());
        final List<String> expected = Arrays.asList("John");
        final SqlParameters parameters = createMock(SqlParameters.class);
        final SqlParameter param1 =createMock(SqlParameter.class);
        final SqlParameter param2 =createMock(SqlParameter.class);
        expect(parameters.next()).andReturn(param1);
        expect(parameters.next()).andReturn(param2);
        param1.setLong(20L);
        param2.setLong(1L);
        replay(parameters, param1, param2);
        final int callCount = whenClause.scroll(new MockQueryEngine<String>(new SqlContext(),
                expected, queryString, parameters),
                new TestCallback<String>("John"));
        assertEquals(1, callCount);
        verify(parameters, param1, param2);
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<String> nick = defineColumn(Mappers.STRING, "nick");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
    }
    
    private static DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

    private static Person person = new Person();
    private static Person person2 = new Person();
    
    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Employee employee = new Employee();
    


}
