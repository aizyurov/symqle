package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractSearchedWhenClause;
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

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class WhenClauseTest extends SqlTestCase {

    public void testSelect() throws Exception {
        final AbstractSearchedWhenClause<String> whenClause = person.age.gt(20L).then(person.name);
        final String sql = whenClause.show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0", sql);
        assertSimilar(sql, whenClause.show(new GenericDialect()));
    }

    public void testMap() throws Exception {
        final AbstractSearchedWhenClause<String> whenClause = person.age.gt(20L).then(person.name);
        final String sql = whenClause.map(Mappers.STRING).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0", sql);
    }
    public void testElse() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orElse(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE T0.nick END AS C0 FROM person AS T0", sql);
    }

    public void testDynamicParameterElse() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orElse(DynamicParameter.create(Mappers.STRING, "do not care")).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE ? END AS C0 FROM person AS T0", sql);
    }

    public void testFunctionElse() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orElse(SqlFunction.create("to_upper", Mappers.STRING).apply(person.nick)).show(new GenericDialect());
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
        final String sql = person.age.gt(20L).then(person.name).selectAll().show(new GenericDialect());
        assertSimilar("SELECT ALL CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0", sql);
    }

    public void testSelectDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).distinct().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).where(person.name.eq("John")).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 WHERE T0.name = ?", sql);

    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).eq(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END = T0.nick", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).ne(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END <> T0.nick", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).gt(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END > T0.nick", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).ge(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END >= T0.nick", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).lt(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END < T0.nick", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).le(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END <= T0.nick", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).eq("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).ne("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).gt("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).ge("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).lt("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).le("John")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END <= ?", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).in(person2.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END IN(SELECT T1.nick FROM person AS T1)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).notIn(person2.nick)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END NOT IN(SELECT T1.nick FROM person AS T1)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).in("John", "James")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).notIn("John", "James")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END NOT IN(?, ?)", sql);
   }

    public void testAsInSubquery() throws Exception {
        final String sql = person2.id.where(person2.nick.in(person.age.gt(20L).then(person.name))).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.nick IN(SELECT CASE WHEN T0.age > ? THEN T0.name END FROM person AS T0)", sql);
    }

    public void testContains() throws Exception {
        final String sql = person2.id.where(person.age.gt(20L).then(person.name).contains("Jim")).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE ? IN(SELECT CASE WHEN T0.age > ? THEN T0.name END FROM person AS T0)", sql);
    }

    public void testAsElseArgument() throws Exception {
        final String sql = person.age.gt(50L).then(person.name).orElse(person.age.lt(20L).then(person.nick)).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name ELSE CASE WHEN T0.age < ? THEN T0.nick END END AS C0 FROM person AS T0", sql);
    }

    public void testSort() throws Exception {
        String sql = person.id.orderBy(person.age.gt(20L).then(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name END", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orderBy(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 ORDER BY T0.nick", sql);
    }

    public void testOrderAsc() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orderAsc().show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 ORDER BY C0 ASC", sql);
    }

    public void testOrderDesc() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).orderDesc().show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 ORDER BY C0 DESC", sql);
    }

    public void testOrderByNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.age.gt(20L).then(person.name).nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name END NULLS FIRST", sql);
    }

    public void testOrderByNullsLast() throws Exception {
        String sql = person.id.orderBy(person.age.gt(20L).then(person.name).nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name END NULLS LAST", sql);
    }

    public void testOrderByDesc() throws Exception {
        String sql = person.id.orderBy(person.age.gt(20L).then(person.name).desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY CASE WHEN T0.age > ? THEN T0.name END DESC", sql);
    }

    public void testOrderByAsc() throws Exception {
        String sql = person.id.orderBy(person.age.gt(20L).then(person.name).asc()).show(new GenericDialect());
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
        final String sql = person.age.gt(20L).then(person.name).concat(person.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END || T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).concat(" test").show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END || ? AS C0 FROM person AS T0", sql);
    }

    public void testSubstring() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).substring(person.id).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name END FROM T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSubstring2() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).substring(person.id, person.id.div(2)).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name END FROM T0.id FOR T0.id / ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).substring(2).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name END FROM ?) AS C0 FROM person AS T0", sql);
    }

    public void testSubstringParam2() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).substring(2, 5).show(new GenericDialect());
        assertSimilar("SELECT SUBSTRING(CASE WHEN T0.age > ? THEN T0.name END FROM ? FOR ?) AS C0 FROM person AS T0", sql);
    }

    public void testPosition() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).positionOf(person.nick).show(new GenericDialect());
        assertSimilar("SELECT POSITION(T0.nick IN CASE WHEN T0.age > ? THEN T0.name END) AS C0 FROM person AS T0", sql);
    }

    public void testPositionParam() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).positionOf("A").show(new GenericDialect());
        assertSimilar("SELECT POSITION(? IN CASE WHEN T0.age > ? THEN T0.name END) AS C0 FROM person AS T0", sql);
    }



    public void testCollate() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).collate("latin1_general_ci").show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END COLLATE latin1_general_ci AS C0 FROM person AS T0", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).union(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 UNION SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).unionAll(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 UNION ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).unionDistinct(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExcept() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).except(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 EXCEPT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).exceptAll(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).exceptDistinct(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }


    public void testIntersect() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).intersect(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 INTERSECT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).intersectAll(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).intersectDistinct(person2.nick).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.nick AS C0 FROM person AS T1", sql);
    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.age.gt(20L).then(person.name).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT CASE WHEN T1.age > ? THEN T1.name END FROM person AS T1)", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.age > ? THEN T0.name END AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = person.age.gt(20L).then(person.name).queryValue().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT(SELECT CASE WHEN T0.age > ? THEN T0.name END FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }
    
    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).isNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END IS NOT NULL", sql);
    }

    public void testLike() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).like(DynamicParameter.create(Mappers.STRING, "J%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END LIKE ?", sql);
    }

    public void testNotLike() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).notLike(DynamicParameter.create(Mappers.STRING, "J%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END NOT LIKE ?", sql);
    }

    public void testLikeString() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).like("J%")).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE CASE WHEN T0.age > ? THEN T0.name END LIKE ?", sql);
    }

    public void testNotLikeString() throws Exception {
        final String sql = person.id.where(person.age.gt(20L).then(person.name).notLike("J%")).show(new GenericDialect());
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

//    public void testList() throws Exception {
//        new Scenario() {
//            @Override
//            protected void runQuery(final AbstractSearchedWhenClause<String> whenClause, final DatabaseGate gate) throws SQLException {
//                final List<String> list = whenClause.list(gate);
//                assertEquals(1, list.size());
//                assertEquals("John", list.get(0));
//            }
//        }.play();
//
//    }
//
//    public void testScroll() throws Exception {
//        new Scenario() {
//            @Override
//            protected void runQuery(final AbstractSearchedWhenClause<String> whenClause, final DatabaseGate gate) throws SQLException {
//                whenClause.scroll(gate, new Callback<String>() {
//                    int callCount = 0;
//
//                    @Override
//                    public boolean iterate(final String aString) {
//                        if (callCount++ != 0) {
//                            fail("One call expected, actually " + callCount);
//                        }
//                        assertEquals("John", aString);
//                        return true;
//                    }
//                });
//            }
//        }.play();
//
//    }

    private static abstract class Scenario {
        public void play() throws Exception {
            final AbstractSearchedWhenClause<String> whenClause = person.age.gt(20L).then(person.name);
            final String queryString = whenClause.show(new GenericDialect());

            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(new GenericDialect());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            statement.setLong(1, 20L);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getString(matches("C[0-9]"))).andReturn("John");
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(gate, connection,  statement, resultSet);

            runQuery(whenClause, gate);
            verify(gate, connection,  statement, resultSet);
        }

        protected abstract void runQuery(final AbstractSearchedWhenClause<String> whenClause, final DatabaseGate gate) throws SQLException;
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
