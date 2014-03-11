package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.Pair;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractQuerySpecification;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 15.12.2013
 * Time: 20:03:17
 * To change this template use File | Settings | File Templates.
 */
public class QuerySpecificationTest extends SqlTestCase {

    private AbstractQuerySpecification<Pair<Long, String>> createQuerySpecification() {
        return person.id.pair(person.name).where(person.name.isNotNull());
    }

    public void testShow() throws Exception {
        final String sql = createQuerySpecification().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testAdapt() throws Exception {
        final String sql = AbstractQuerySpecification.adapt(person.id.pair(person.name)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testLimit() throws Exception {
        final String sql = createQuerySpecification().limit(20).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createQuerySpecification().limit(10,20).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = createQuerySpecification().forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createQuerySpecification().forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL FOR READ ONLY", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = createQuerySpecification().orderBy(person.name).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 WHERE T0.name IS NOT NULL ORDER BY T0.name", sql);
    }

    public void testList() throws Exception {
        new PairScenario<AbstractQuerySpecification<Pair<Long, String>>>(createQuerySpecification()) {
            @Override
            protected void use(AbstractQuerySpecification<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
                final List<Pair<Long, String>> expected = Arrays.asList(Pair.make(123L, "John"));
                assertEquals(expected, query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new PairScenario<AbstractQuerySpecification<Pair<Long, String>>>(createQuerySpecification()) {
            @Override
            protected void use(AbstractQuerySpecification<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Pair<Long,String>>(Pair.make(123L, "John"))));
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new PairScenario<AbstractQuerySpecification<Pair<Long, String>>>(createQuerySpecification()) {
            @Override
            protected void use(AbstractQuerySpecification<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
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
