package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractAggregateFunction;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.expect;

/**
 * @author lvovich
 */
public class AggregatesTest extends SqlTestCase  {

    public void testShow() throws Exception {
        final String show = person.id.count().showQuery(new GenericDialect());
        final String show2 = person.id.count().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1", show);
        assertSimilar(show, show2);
    }

    public void testAdapt() throws Exception {
        final AbstractAggregateFunction<Integer> function = person.id.count();
        final AbstractAggregateFunction<Integer> adaptor = AbstractAggregateFunction.adapt(function);
        assertEquals(function.showQuery(new GenericDialect()), adaptor.showQuery(new GenericDialect()));
        assertEquals(function.getMapper(), adaptor.getMapper());
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.count().forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.count().forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 FOR READ ONLY", sql);
    }

    public void testLimit() throws Exception {
        final String sql = person.id.count().limit(1).showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 FETCH FIRST 1 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = person.id.count().limit(1, 2).showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 OFFSET 1 ROWS FETCH FIRST 2 ROWS ONLY", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.count().where(person.name.isNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.name IS NULL", sql);
    }

    public void testExists() throws Exception {
        // rather meaningless
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(child.age.sum().exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE EXISTS(SELECT SUM(T2.age) FROM person AS T2)", sql);
    }

    public void testContains() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(child.age.sum().contains(1)).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE ? IN(SELECT SUM(T2.age) FROM person AS T2)", sql);
    }

    public void testIn() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(DynamicParameter.create(CoreMappers.INTEGER, 1).in(child.id.count())).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE ? IN(SELECT COUNT(T2.id) FROM person AS T2)", sql);
    }

    public void testQueryValue() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.pair(child.id.count().queryValue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1,(SELECT COUNT(T2.id) FROM person AS T2) AS C2 FROM person AS T1", sql);
    }

    public void testAll() throws Exception {
        final Person person = new Person();
        final Person senior = new Person();
        final String sql = person.name.where(person.age.eq(senior.age.max().all())).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE T1.age = ALL(SELECT MAX(T2.age) FROM person AS T2)", sql);
    }

    public void testAny() throws Exception {
        final Person person = new Person();
        final Person senior = new Person();
        final String sql = person.name.where(person.age.eq(senior.age.max().any())).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE T1.age = ANY(SELECT MAX(T2.age) FROM person AS T2)", sql);
    }

    public void testSome() throws Exception {
        final Person person = new Person();
        final Person senior = new Person();
        final String sql = person.name.where(person.age.eq(senior.age.max().some())).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE T1.age = SOME(SELECT MAX(T2.age) FROM person AS T2)", sql);
    }



    public void testList() throws Exception {
        new Scenario(person.id.count()) {
            @Override
            void use(AbstractAggregateFunction<Integer> query, QueryEngine engine) throws SQLException {
                final List<Integer> expected = Arrays.asList(123);
                assertEquals(expected, query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(person.id.count()) {
            @Override
            void use(AbstractAggregateFunction<Integer> query, QueryEngine engine) throws SQLException {
                int rows = query.scroll(engine, new TestCallback<Integer>(123));
                assertEquals(1, rows);
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(person.id.count()) {
            @Override
            void use(AbstractAggregateFunction<Integer> query, QueryEngine engine) throws SQLException {
                int rows = query.compileQuery(engine).scroll(new TestCallback<Integer>(123));
                assertEquals(1, rows);
            }
        }.play();
    }

    public static abstract class Scenario extends AbstractQueryScenario<Integer,AbstractAggregateFunction<Integer>> {

        protected Scenario(AbstractAggregateFunction<Integer> query) {
            super(query, "C0");
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getInt()).andReturn(123);
        }
    }

    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }

        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<Long> age = defineColumn(CoreMappers.LONG, "age");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<Long> parentId = defineColumn(CoreMappers.LONG, "parent_id");
    }

    private static Person person = new Person();

}
