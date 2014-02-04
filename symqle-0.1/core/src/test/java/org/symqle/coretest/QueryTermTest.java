package org.symqle.coretest;

import org.symqle.common.*;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class QueryTermTest extends SqlTestCase {

    private AbstractQueryTerm<Long> createQueryTerm() {
        return employee.id.intersect(manager.id);
    }

    public void testShow() throws Exception {
        final AbstractQueryTerm<Long> queryTerm = createQueryTerm();
        final String sql = queryTerm.show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2", sql);
        assertSimilar(sql, queryTerm.show(new GenericDialect()));
    }

    public void testAdapt() throws Exception {
        final AbstractQueryTerm<Long> adaptor = AbstractQueryTerm.adapt(employee.id);
        final String sql = adaptor.show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1", sql);
        assertEquals(adaptor.getMapper(), employee.id.getMapper());
    }

    public void testOrderBy() throws Exception {
        final AbstractQueryTerm<Long> queryTerm = employee.id.intersect(manager.id);
        try {
            final String sql = queryTerm.orderBy(employee.name).show(new GenericDialect());
            fail ("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // Ok
        }
        try {
            final String sql = queryTerm.orderBy(manager.name).show(new GenericDialect());
            fail ("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // Ok
        }
    }

    public void testLimit() throws Exception {
        final String sql = createQueryTerm().limit(20).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createQueryTerm().limit(10, 20).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = createQueryTerm().queryValue().where(person.name.isNull()).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T1.id FROM employee AS T1 INTERSECT SELECT T2.id FROM manager AS T2) AS C1 FROM person AS T3 WHERE T3.name IS NULL", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = createQueryTerm().forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createQueryTerm().forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 FOR READ ONLY", sql);
    }

    public void testUnion() throws Exception {
        final String sql = createQueryTerm().union(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 UNION SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = createQueryTerm().unionAll(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 UNION ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = createQueryTerm().unionDistinct(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 UNION DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExcept() throws Exception {
        final String sql = createQueryTerm().except(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 EXCEPT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = createQueryTerm().exceptAll(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 EXCEPT ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = createQueryTerm().exceptDistinct(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 EXCEPT DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = createQueryTerm().intersect(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 INTERSECT SELECT T0.id AS C0 FROM person AS T0", sql);
    }

    public void testIntersectWithIntersection() throws Exception {
        final String sql = employee.id.intersect((manager.id).intersect(person.id)).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT(SELECT T2.id AS C0 FROM manager AS T2 INTERSECT SELECT T0.id AS C0 FROM person AS T0)", sql);
    }

    public void testIntersectAll() throws Exception {
        final String sql = createQueryTerm().intersectAll(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 INTERSECT ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = createQueryTerm().intersectDistinct(person.id).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 INTERSECT SELECT T2.id AS C0 FROM manager AS T2 INTERSECT DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExists() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.id.eq(person.id)).intersect(manager.id.where(manager.id.eq(person.id))).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.id = T0.id INTERSECT SELECT T2.id FROM manager AS T2 WHERE T2.id = T0.id)", sql);
    }

    public void testContains() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.id.eq(person.id)).intersect(manager.id.where(manager.id.eq(person.id))).contains(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.id FROM employee AS T1 WHERE T1.id = T0.id INTERSECT SELECT T2.id FROM manager AS T2 WHERE T2.id = T0.id)", sql);
    }

    public void testAsInSublist() throws Exception {
        final String sql = person.name.where(person.id.in(createQueryTerm())).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE T0.id IN(SELECT T1.id FROM employee AS T1 INTERSECT SELECT T2.id FROM manager AS T2)", sql);
    }


    public void testList() throws Exception {
        new Scenario(createQueryTerm()) {
            @Override
            void use(AbstractQueryTerm<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(Arrays.asList(123L), query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(createQueryTerm()) {
            @Override
            void use(AbstractQueryTerm<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Long>(123L)));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<Long, AbstractQueryTerm<Long>> {
        protected Scenario(AbstractQueryTerm<Long> query) {
            super(query, "C0");
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getLong()).andReturn(123L);
        }
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

    private static Person person = new Person();

    private static Employee employee = new Employee();

    private static Manager manager = new Manager();

    private static Person person2 = new Person();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
