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
public class AggregateQuerySpecificationTest extends SqlTestCase {

    public void testShow() throws Exception {
        final String sql = aggregateQuery.showQuery(new GenericDialect());
        final String sql2 = aggregateQuery.showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ?", sql);
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ?", sql2);
    }

    public void testAdapt() throws Exception {
        final AbstractAggregateFunction<Integer> count = employee.id.count();
        final AbstractAggregateQuerySpecification<Integer> adaptor = AbstractAggregateQuerySpecification.adapt(count);
        final GenericDialect dialect = new GenericDialect();
        assertSimilar(count.showQuery(dialect), adaptor.showQuery(dialect));
        assertEquals(count.getMapper(), adaptor.getMapper());
    }

    public void testLimit() throws Exception {
        final String sql = aggregateQuery.limit(1).showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? FETCH FIRST 1 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = aggregateQuery.limit(1, 1).showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? OFFSET 1 ROWS FETCH FIRST 1 ROWS ONLY", sql);
    }

    public void testContains() throws Exception {
        final String sql = new Employee().name.where(aggregateQuery.contains(1)).showQuery(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testInArgument() throws Exception {
        final String sql = new Employee().name.where(DynamicParameter.create(CoreMappers.INTEGER, 1).in(aggregateQuery)).showQuery(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE ? IN(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testExists() throws Exception {
        final String sql = new Employee().name.where(aggregateQuery.exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE EXISTS(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = aggregateQuery.forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? FOR READ ONLY", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = aggregateQuery.forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM employee AS T1 WHERE T1.name LIKE ? FOR UPDATE", sql);
    }

    public void testQueryValue() throws Exception {
        final String sql = aggregateQuery.queryValue().orderBy(new Employee().id).showQuery(new GenericDialect());
        assertSimilar("SELECT(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?) AS C1 FROM employee AS T2 ORDER BY T2.id", sql);
        final AbstractAggregateQuerySpecification<Integer> where = employee.id.count().where(employee.name.like("A%"));
    }

    public void testList() throws Exception {
        new Scenario(aggregateQuery) {
            @Override
            void use(AbstractAggregateQuerySpecification<Integer> query, QueryEngine engine) throws SQLException {
                final List<Integer> expected = Arrays.asList(123);
                final List<Integer> list = aggregateQuery.list(
                        engine);
                assertEquals(expected, list);
            }
        }.play();
    }

    public void testAll() throws Exception {
        final Employee employee = new Employee();
        final Employee a = new Employee();
        final AbstractAggregateQuerySpecification<String> aggregateQuerySpecification = a.name.max().where(a.name.like("A"));
        final String sql = employee.name.where(employee.name.eq(aggregateQuerySpecification.all())).showQuery(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE T2.name = ALL(SELECT MAX(T1.name) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testAny() throws Exception {
        final Employee employee = new Employee();
        final Employee a = new Employee();
        final AbstractAggregateQuerySpecification<String> aggregateQuerySpecification = a.name.max().where(a.name.like("A"));
        final String sql = employee.name.where(employee.name.eq(aggregateQuerySpecification.any())).showQuery(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE T2.name = ANY(SELECT MAX(T1.name) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testSome() throws Exception {
        final Employee employee = new Employee();
        final Employee a = new Employee();
        final AbstractAggregateQuerySpecification<String> aggregateQuerySpecification = a.name.max().where(a.name.like("A"));
        final String sql = employee.name.where(employee.name.eq(aggregateQuerySpecification.some())).showQuery(new GenericDialect());
        assertSimilar("SELECT T2.name AS C2 FROM employee AS T2 WHERE T2.name = SOME(SELECT MAX(T1.name) FROM employee AS T1 WHERE T1.name LIKE ?)", sql);
    }

    public void testScroll() throws Exception {
        new Scenario(aggregateQuery) {
            @Override
            void use(AbstractAggregateQuerySpecification<Integer> query, QueryEngine engine) throws SQLException {
                int rows = aggregateQuery.scroll(
                    engine, new TestCallback<Integer>(123));
                assertEquals(1, rows);
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(aggregateQuery) {
            @Override
            void use(AbstractAggregateQuerySpecification<Integer> query, QueryEngine engine) throws SQLException {
                int rows = aggregateQuery.compileQuery(engine).scroll(
                    new TestCallback<Integer>(123));
                assertEquals(1, rows);
            }
        }.play();
    }

    private abstract class Scenario extends AbstractQueryScenario<Integer, AbstractAggregateQuerySpecification<Integer>> {

        protected Scenario(AbstractAggregateQuerySpecification<Integer> query) {
            super(query, "C0");
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            final OutBox param =createMock(OutBox.class);
            expect(parameters.next()).andReturn(param);
            param.setString("A%");
            return Collections.singletonList(param);
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getInt()).andReturn(123);
        }

    }

    private static class Employee extends TableOrView {
        @Override
        public String getTableName() {
            return "employee";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");

    }

    private static final Employee employee = new Employee();
    private static final AbstractAggregateQuerySpecification<Integer> aggregateQuery = employee.id.count().where(employee.name.like("A%"));


}
