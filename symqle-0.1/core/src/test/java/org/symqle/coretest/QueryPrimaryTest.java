package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameter;
import org.symqle.common.SqlParameters;
import org.symqle.sql.AbstractQueryPrimary;
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
public class QueryPrimaryTest extends SqlTestCase {

    public void testShow() throws Exception {
        final String sql = queryPrimary.show(new GenericDialect());
        final String sql2 = queryPrimary.show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ?", sql);
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ?", sql2);
    }

    public void testContains() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testInArgument() throws Exception {
        final String sql = new Employee().name.where(DynamicParameter.create(Mappers.INTEGER, 1).in(queryPrimary)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testExcept() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.except(queryPrimary).contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? EXCEPT SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testExceptAll() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.exceptAll(queryPrimary).contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? EXCEPT ALL SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testExceptDistinct() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.exceptDistinct(queryPrimary).contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? EXCEPT DISTINCT SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testIntersect() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.intersect(queryPrimary).contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? INTERSECT SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.intersectAll(queryPrimary).contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? INTERSECT ALL SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testIntersectDistinct() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.intersectDistinct(queryPrimary).contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? INTERSECT DISTINCT SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testUnion() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.union(queryPrimary).contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? UNION SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testUnionAll() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.unionAll(queryPrimary).contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? UNION ALL SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testUnionDistinct() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.unionDistinct(queryPrimary).contains(1)).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ? UNION DISTINCT SELECT COUNT(T1.id) FROM employee AS T3 WHERE T3.name LIKE ?)", sql);
    }

    public void testExists() throws Exception {
        final String sql = new Employee().name.where(queryPrimary.exists()).show(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE EXISTS(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = queryPrimary.forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? FOR READ ONLY", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = queryPrimary.forUpdate().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? FOR UPDATE", sql);
    }

    public void testOrderAsc() throws Exception {
        final String sql = queryPrimary.orderAsc().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? ORDER BY C1 ASC", sql);
    }

    public void testOrderDesc() throws Exception {
        final String sql = queryPrimary.orderDesc().show(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? ORDER BY C1 DESC", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = queryPrimary.queryValue().orderBy(new Employee().id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?) AS C1 FROM employee AS T2 ORDER BY T2.id", sql);
    }

    public void testList() throws Exception {
        final String queryString = queryPrimary.show(new GenericDialect());
        final List<Integer> expected = Arrays.asList(123);
        final SqlParameters parameters = createMock(SqlParameters.class);
        final SqlParameter param =createMock(SqlParameter.class);
        expect(parameters.next()).andReturn(param);
        param.setString("A%");
        replay(parameters, param);
        final List<Integer> list = queryPrimary.list(
            new MockQueryEngine<Integer>(new SqlContext(), expected, queryString, parameters));
        assertEquals(expected, list);
        verify(parameters, param);
    }

    public void testScroll() throws Exception {
        final String queryString = queryPrimary.show(new GenericDialect());
        final List<Integer> expected = Arrays.asList(123);
        final SqlParameters parameters = createMock(SqlParameters.class);
        final SqlParameter param =createMock(SqlParameter.class);
        expect(parameters.next()).andReturn(param);
        param.setString("A%");
        replay(parameters, param);
        int rows = queryPrimary.scroll(
            new MockQueryEngine<Integer>(new SqlContext(),
                    expected, queryString, parameters),
                new TestCallback<Integer>(123));
        assertEquals(1, rows);
        verify(parameters, param);
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static final Employee employee = new Employee();
    private static final AbstractQueryPrimary<Integer> queryPrimary = employee.id.count().where(employee.name.like("A%"));


}
