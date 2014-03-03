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
        final String sql = new Employee().name.where(DynamicParameter.create(CoreMappers.INTEGER, 1).in(queryPrimary)).show(new GenericDialect());
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

    public void testQueryValue() throws Exception {
        final String sql = queryPrimary.queryValue().orderBy(new Employee().id).show(new GenericDialect());
        assertSimilar("SELECT(SELECT COUNT(T1.id) FROM employee AS T1 WHERE T1.name LIKE ?) AS C1 FROM employee AS T2 ORDER BY T2.id", sql);
    }

    public void testList() throws Exception {
        new Scenario(queryPrimary) {
            @Override
            void use(AbstractQuerySpecificationScalar<Integer> query, QueryEngine engine) throws SQLException {
                final List<Integer> expected = Arrays.asList(123);
                final List<Integer> list = queryPrimary.list(
                        engine);
                assertEquals(expected, list);
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(queryPrimary) {
            @Override
            void use(AbstractQuerySpecificationScalar<Integer> query, QueryEngine engine) throws SQLException {
                int rows = queryPrimary.scroll(
                    engine, new TestCallback<Integer>(123));
                assertEquals(1, rows);
            }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(queryPrimary) {
            @Override
            void use(AbstractQuerySpecificationScalar<Integer> query, QueryEngine engine) throws SQLException {
                int rows = queryPrimary.compileQuery(engine).scroll(
                    new TestCallback<Integer>(123));
                assertEquals(1, rows);
            }
        }.play();
    }

    private abstract class Scenario extends AbstractQueryScenario<Integer, AbstractQuerySpecificationScalar<Integer>> {

        protected Scenario(AbstractQuerySpecificationScalar<Integer> query) {
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
    private static final AbstractQuerySpecificationScalar<Integer> queryPrimary = employee.id.count().where(employee.name.like("A%"));


}
