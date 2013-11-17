package org.symqle.coretest;

import org.symqle.common.Element;
import org.symqle.common.Mappers;
import org.symqle.common.Row;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.sql.AbstractAggregateFunction;
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
        final AbstractAggregateFunction<Integer> count = person.id.count();
        final String queryString = count.show(new GenericDialect());
        final List<Integer> expected = Arrays.asList(2);
        final SqlParameters parameters = createMock(SqlParameters.class);
        replay(parameters);
        final List<Integer> list = count.list(
            new MockQueryEngine<Integer>(new SqlContext(), expected, queryString, parameters));
        assertEquals(expected, list);
        verify(parameters);
    }

    public void testExtract() throws Exception {
        final AbstractAggregateFunction<Integer> count = person.id.count();
        final String queryString = count.show(new GenericDialect());
        final Row row = createMock(Row.class);
        final Element element = createMock(Element.class);
        final List<Row> rows = Arrays.asList(row);
        final SqlParameters parameters = createMock(SqlParameters.class);
        expect(row.getValue("S0")).andReturn(element);
        expect(element.getInt()).andReturn(123);
        replay(parameters, row, element);
        final List<Integer> list = count.list(
                new MockRowsQueryEngine(queryString, parameters, new SqlContext(), rows));
        assertEquals(Arrays.asList(123),  list);
        verify(parameters, row, element);

    }

    public void testScroll() throws Exception {
        final AbstractAggregateFunction<Integer> count = person.id.count();
        final String queryString = count.show(new GenericDialect());
        final List<Integer> expected = Arrays.asList(2);
        final SqlParameters parameters = createMock(SqlParameters.class);
        replay(parameters);
        int rows = count.scroll(
            new MockQueryEngine<Integer>(new SqlContext(),
                    expected, queryString, parameters),
                new TestCallback<Integer>(2));
        assertEquals(1, rows);
        verify(parameters);
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
