package org.symqle.coretest;

import org.symqle.common.*;
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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class AggregatesTest extends SqlTestCase  {

    public void testShow() throws Exception {
        final String show = person.id.count().show(new GenericDialect());
        final String show2 = person.id.count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1", show);
        assertSimilar(show, show2);
    }

    public void testUnion() throws Exception {
        final String show = person.id.count().union(person.parentId.count()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 UNION SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testUnionAll() throws Exception {
        final String show = person.id.count().unionAll(person.parentId.count()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 UNION ALL SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }
    public void testUnionDistinct() throws Exception {
        final String show = person.id.count().unionDistinct(person.parentId.count()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 UNION DISTINCT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testExcept() throws Exception {
        final String show = person.id.count().except(person.parentId.count()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 EXCEPT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testExceptAll() throws Exception {
        final String show = person.id.count().exceptAll(person.parentId.count()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 EXCEPT ALL SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testExceptDistinct() throws Exception {
        final String show = person.id.count().exceptDistinct(person.parentId.count()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 EXCEPT DISTINCT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testIntersect() throws Exception {
        final String show = person.id.count().intersect(person.parentId.count()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 INTERSECT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testIntersectAll() throws Exception {
        final String show = person.id.count().intersectAll(person.parentId.count()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 INTERSECT ALL SELECT COUNT(T2.parent_id) AS C2 FROM person AS T2", show);
    }

    public void testIntersectDistinct() throws Exception {
        final AbstractAggregateFunction<Integer> count = person.id.count();
        final String show = count.intersectDistinct(person.parentId.count()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 INTERSECT DISTINCT SELECT COUNT(T2.parent_id) AS C2 FROM person AS T2", show);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.count().forUpdate().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.count().forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 FOR READ ONLY", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.count().where(person.name.isNull()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.name IS NULL", sql);
    }

    public void testOrderAsc() throws Exception {
        final String sql = person.id.count().orderAsc().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 ORDER BY C1 ASC", sql);
    }

    public void testOrderDesc() throws Exception {
        final String sql = person.id.count().orderDesc().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 ORDER BY C1 DESC", sql);
    }

    public void testExists() throws Exception {
        // rather meaningless
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(child.age.sum().exists()).show(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE EXISTS(SELECT SUM(T2.age) FROM person AS T2)", sql);
    }

    public void testContains() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(child.age.sum().contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE ? IN(SELECT SUM(T2.age) FROM person AS T2)", sql);
    }

    public void testIn() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(DynamicParameter.create(Mappers.INTEGER, 1).in(child.id.count())).show(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE ? IN(SELECT COUNT(T2.id) FROM person AS T2)", sql);
    }

    public void testQueryValue() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.pair(child.id.count().queryValue()).show(new GenericDialect());
        assertSimilar("SELECT T1.name AS C1,(SELECT COUNT(T2.id) FROM person AS T2) AS C2 FROM person AS T1", sql);
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

    public static abstract class Scenario extends AbstractQueryScenario<Integer,AbstractAggregateFunction<Integer>> {

        protected Scenario(AbstractAggregateFunction<Integer> query) {
            super(query, "S0");
        }

        @Override
        List<SqlParameter> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(Element element) throws SQLException {
            expect(element.getInt()).andReturn(123);
        }
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
    }

    private static Person person = new Person();

}
