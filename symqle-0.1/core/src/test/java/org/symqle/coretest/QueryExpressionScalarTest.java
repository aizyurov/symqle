package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.sql.AbstractQueryExpressionScalar;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class QueryExpressionScalarTest extends SqlTestCase {


    public void testQueryValueBooleanValue() throws Exception {
        final String sql = person.id.where(employee.id.union(manager.id).queryValue().asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2)", sql);
    }

    public void testShow() throws Exception {
        final String sql = employee.id.union(manager.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2", sql);
        final String sql2 = employee.id.union(manager.id).show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testForUpdate() throws Exception {
        final String sql = employee.id.union(manager.id).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = employee.id.union(manager.id).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 FOR READ ONLY", sql);
    }

    public void testOrderAsc() throws Exception {
        final String sql = employee.id.union(manager.id).orderAsc().show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 ORDER BY C0 ASC", sql);
    }

    public void testOrderDesc() throws Exception {
        final String sql = employee.id.union(manager.id).orderDesc().show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 ORDER BY C0 DESC", sql);
    }

    public void testUnion() throws Exception {
        final String sql = employee.id.union(manager.id).union(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = employee.id.union(manager.id).unionAll(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = employee.id.union(manager.id).unionDistinct(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExcept() throws Exception {
        final String sql = employee.id.union(manager.id).except(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = employee.id.union(manager.id).exceptAll(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = employee.id.union(manager.id).exceptDistinct(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = employee.id.union(manager.id).intersect(person.id).show(new GenericDialect());
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = employee.id.union(manager.id).intersectAll(person.id).show(new GenericDialect());
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = employee.id.union(manager.id).intersectDistinct(person.id).show(new GenericDialect());
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExists() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.id.eq(person.id)).union(manager.id.where(manager.id.eq(person.id))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.id = T0.id UNION SELECT T2.id FROM manager AS T2 WHERE T2.id = T0.id)", sql);

    }

    public void testAsInSublist() throws Exception {
        final String sql = person.name.where(person.id.in(employee.id.except(manager.id))).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE T0.id IN(SELECT T1.id FROM employee AS T1 EXCEPT SELECT T2.id FROM manager AS T2)", sql);
    }

    public void testContains() throws Exception {
        final String sql = person.name.where(employee.id.except(manager.id).contains(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.id FROM employee AS T1 EXCEPT SELECT T2.id FROM manager AS T2)", sql);
    }



    public void testList() throws Exception {
        final AbstractQueryExpressionScalar<Long> queryExpressionScalar = employee.id.union(manager.id);
        final String queryString = queryExpressionScalar.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        replay(parameters);
        final List<Long> expected = Arrays.asList(123L);
        final List<Long> list = queryExpressionScalar.list(
            new MockQueryEngine<Long>(new SqlContext(), expected, queryString, parameters));
        assertEquals(expected, list);
        verify(parameters);
    }

    public void testScroll() throws Exception {
        final AbstractQueryExpressionScalar<Long> queryExpressionScalar = employee.id.union(manager.id);
        final String queryString = queryExpressionScalar.show(new GenericDialect());
        final List<Long> expected = Arrays.asList(123L);
        final SqlParameters parameters = createMock(SqlParameters.class);
        replay(parameters);
        int rows = queryExpressionScalar.scroll(
            new MockQueryEngine<Long>(new SqlContext(),
                    expected, queryString, parameters),
                new TestCallback<Long>(123L));
        assertEquals(1, rows);
        verify(parameters);
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static class Manager extends TableOrView {
        private Manager() {
            super("manager");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Manager manager = new Manager();

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
