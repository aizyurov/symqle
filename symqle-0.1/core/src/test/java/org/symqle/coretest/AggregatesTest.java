package org.symqle.coretest;

import org.symqle.common.InBox;
import org.symqle.common.Mappers;
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
        final String show = person.id.count().show(new GenericDialect());
        final String show2 = person.id.count().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1", show);
        assertSimilar(show, show2);
    }

    public void testAdapt() throws Exception {
        final AbstractAggregateFunction<Integer> function = person.id.count();
        final AbstractAggregateFunction<Integer> adaptor = AbstractAggregateFunction.adapt(function);
        assertEquals(function.show(new GenericDialect()), adaptor.show(new GenericDialect()));
        assertEquals(function.getMapper(), adaptor.getMapper());
    }

    public void testDistinct() throws Exception {
        final String sql = person.id.count().distinct().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT COUNT(T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testSelectAll() throws Exception {
        final String sql = person.id.count().selectAll().show(new GenericDialect());
        assertSimilar("SELECT ALL COUNT(T1.id) AS C1 FROM person AS T1", sql);
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

    public void testLimit() throws Exception {
        final String sql = person.id.count().limit(1).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 FETCH FIRST 1 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = person.id.count().limit(1, 2).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 OFFSET 1 ROWS FETCH FIRST 2 ROWS ONLY", sql);
    }

    public void testOrderBy() throws Exception {
        // incorrect statement! must have GROUP BY
        // will throw SQLException with real database
        final String sql = person.id.count().orderBy(person.name).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T0.id) AS C0 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.id.count().where(person.name.isNull()).show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.name IS NULL", sql);
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
