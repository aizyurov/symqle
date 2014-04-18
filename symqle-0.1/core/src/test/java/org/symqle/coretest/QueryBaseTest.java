package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.Pair;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 15.12.2013
 * Time: 19:06:10
 * To change this template use File | Settings | File Templates.
 */
public class QueryBaseTest extends SqlTestCase {

    private AbstractQueryBase<Pair<Long, String>> createQueryBase() {
        return person.id.pair(person.name).distinct();
    }

    public void testShow() throws Exception {
        final String sql = createQueryBase().showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testAdapt() throws Exception {
        final String sql = AbstractQueryBase.adapt(person.id.pair(person.name)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testWhere() throws Exception {
        final String sql = createQueryBase().where(person.name.isNotNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = createQueryBase().orderBy(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = createQueryBase().forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createQueryBase().forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testLimit() throws Exception {
        final String sql = createQueryBase().limit(10).showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0 FETCH FIRST 10 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createQueryBase().limit(10, 20).showQuery(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0, T0.name AS C1 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testCountRows() throws Exception {
        final String sql = createQueryBase().countRows().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(*) AS C0 FROM(SELECT DISTINCT T0.id, T0.name FROM person AS T0) AS T1", sql);
    }

    public void testList() throws Exception {
        new PairScenario<AbstractQueryBase<Pair<Long, String>>>(createQueryBase()) {
            @Override
            protected void use(AbstractQueryBase<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
                final List<Pair<Long, String>> expected = Arrays.asList(Pair.make(123L, "John"));
                assertEquals(expected, query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new PairScenario<AbstractQueryBase<Pair<Long, String>>>(createQueryBase()) {
            @Override
            protected void use(AbstractQueryBase<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Pair<Long,String>>(Pair.make(123L, "John"))));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new PairScenario<AbstractQueryBase<Pair<Long, String>>>(createQueryBase()) {
            @Override
            protected void use(AbstractQueryBase<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine).scroll(new TestCallback<Pair<Long,String>>(Pair.make(123L, "John"))));
            }
        }.play();
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
