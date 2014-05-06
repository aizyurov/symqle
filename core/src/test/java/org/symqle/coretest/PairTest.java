package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.MalformedStatementException;
import org.symqle.common.Pair;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 01.01.2013
 * Time: 13:30:17
 * To change this template use File | Settings | File Templates.
 */
public class PairTest extends SqlTestCase {

    private AbstractSelectList<Pair<Long, String>> createSelectList() {
        return person.id.pair(person.name);
    }

    public void testShow() throws Exception {
        final String sql = createSelectList().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
        final String sql2 = createSelectList().showQuery(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testAdapt() throws Exception {
        final String sql = AbstractSelectList.adapt(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0", sql);
    }

    public void testNoTablesMysqlLikeDialect() throws Exception {
        final String sql = Symqle.currentDate().pair(Symqle.currentTime()).showQuery(new MysqlLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT CURRENT_DATE AS C0, CURRENT_TIME AS C1", sql);
        final String sql2 = Symqle.currentDate().pair(Symqle.currentTime()).distinct().showQuery(new MysqlLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT DISTINCT CURRENT_DATE AS C0, CURRENT_TIME AS C1", sql2);
        final String sql3 = Symqle.currentDate().pair(Symqle.currentTime()).selectAll().showQuery(new MysqlLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT ALL CURRENT_DATE AS C0, CURRENT_TIME AS C1", sql3);
    }


    public void testPairChain() throws Exception {
        final String sql = createSelectList().pair(person.age).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1, T0.age AS C2 FROM person AS T0", sql);
    }

    public void testPairArgument() throws Exception {
        final String sql = person.id.pair(person.name.pair(person.age)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1, T0.age AS C2 FROM person AS T0", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = createSelectList().selectAll().showQuery(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
        final String sql2 = createSelectList().selectAll().showQuery(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testWhere() throws Exception {
        final AbstractQuerySpecification<Pair<Long, String>> querySpecification = createSelectList().where(person.name.isNotNull());
        final String sql = querySpecification.showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
        final String sql2 = querySpecification.showQuery(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testOrderBy() throws Exception {
        final String sql = createSelectList().orderBy(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testLimit() throws Exception {
        final String sql = createSelectList().limit(20).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createSelectList().limit(10, 20).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = createSelectList().forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createSelectList().forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testOrderByForUpdate() throws Exception {
        final String sql = createSelectList().orderBy(person.name).forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 ORDER BY T0.name FOR UPDATE", sql);
    }

    public void testOrderByForReadOnly() throws Exception {
        final String sql = createSelectList().orderBy(person.name).forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 ORDER BY T0.name FOR READ ONLY", sql);
    }

    public void testCountRows() throws Exception {
        final String sql = createSelectList().countRows().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(*) AS C0 FROM(SELECT T0.id, T0.name FROM person AS T0) AS T3", sql);
    }

    public void testImplicitCrossJoin() throws Exception {
        final Person person1 = new Person();
        final Person person2 = new Person();
        final String badSql;
        try {
            badSql = person1.name.pair(person2.age).where(person1.id.eq(person2.id)).showQuery(new GenericDialect());
            fail("expected IllegalStateException but was " + badSql);
        } catch (MalformedStatementException e) {
            // fine
        }
        final String goodSql = person1.name.pair(person2.age).where(person1.id.eq(person2.id)).showQuery(new GenericDialect(), Option.allowImplicitCrossJoins(true));
        assertSimilar("SELECT T1.name AS C1, T2.age AS C2 FROM person AS T1, person AS T2 WHERE T1.id = T2.id", goodSql);
    }

    public void testList() throws Exception {
        new PairScenario<AbstractSelectList<Pair<Long, String>>>(createSelectList()) {
            @Override
            protected void use(AbstractSelectList<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
                final List<Pair<Long, String>> expected = Arrays.asList(Pair.make(123L, "John"));
                assertEquals(expected, query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new PairScenario<AbstractSelectList<Pair<Long, String>>>(createSelectList()) {
            @Override
            protected void use(AbstractSelectList<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Pair<Long,String>>(Pair.make(123L, "John"))));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new PairScenario<AbstractSelectList<Pair<Long, String>>>(createSelectList()) {
            @Override
            protected void use(AbstractSelectList<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
                final List<Pair<Long, String>> expected = Arrays.asList(Pair.make(123L, "John"));
                assertEquals(expected, query.compileQuery(engine).list());
            }
        }.play();
    }


    public void testToString() throws Exception {
        assertEquals("(1, John)", Pair.make(1, "John").toString());
    }

    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<Long> age = defineColumn(CoreMappers.LONG, "age");
    }

    private static Person person = new Person();

}
