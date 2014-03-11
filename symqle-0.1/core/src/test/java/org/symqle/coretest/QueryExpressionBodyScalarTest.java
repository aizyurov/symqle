package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.MalformedStatementException;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractQueryExpressionBodyScalar;
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
public class QueryExpressionBodyScalarTest extends SqlTestCase {


    public void testQueryValueBooleanValue() throws Exception {
        final String sql = person.id.where(createQueryExpressionBodyScalar().queryValue().asPredicate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1 UNION SELECT T2.id FROM manager AS T2)", sql);
    }

    private AbstractQueryExpressionBodyScalar<Long> createQueryExpressionBodyScalar() {
        return employee.id.union(manager.id);
    }

    public void testShow() throws Exception {
        final AbstractQueryExpressionBodyScalar<Long> queryExpressionBodyScalar = createQueryExpressionBodyScalar();
        final String sql = queryExpressionBodyScalar.showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2", sql);
        final String sql2 = createQueryExpressionBodyScalar().showQuery(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testAdapt() throws Exception {
        final AbstractQueryExpressionBodyScalar<Long> adaptor = AbstractQueryExpressionBodyScalar.adapt(employee.id);
        final String sql = adaptor.showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1", sql);
        assertEquals(adaptor.getMapper(), employee.id.getMapper());
    }

    public void testForUpdate() throws Exception {
        final String sql = createQueryExpressionBodyScalar().forUpdate().showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createQueryExpressionBodyScalar().forReadOnly().showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 FOR READ ONLY", sql);
    }

    public void testLimit() throws Exception {
        final String sql = createQueryExpressionBodyScalar().limit(10).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 FETCH FIRST 10 ROWS ONLY", sql);
        
    }

    public void testLimit2() throws Exception {
        final String sql = createQueryExpressionBodyScalar().limit(10,20).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);

    }

    public void testOrderBy() throws Exception {
        try {
            final String sql = employee.id.union(manager.id).orderBy(employee.name).showQuery(new GenericDialect());
            fail ("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // Ok
        }
        try {
            final String sql = employee.id.union(manager.id).orderBy(employee.name).showQuery(new MysqlLikeDialect(), Option.allowNoTables(true));
            fail ("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // Ok
        }
        try {
            final String sql = employee.id.union(manager.id).orderBy(manager.name).showQuery(new GenericDialect());
            fail ("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // Ok
        }
    }

    public void testUnion() throws Exception {
        final String sql = createQueryExpressionBodyScalar().union(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = createQueryExpressionBodyScalar().unionAll(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = createQueryExpressionBodyScalar().unionDistinct(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 UNION DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExcept() throws Exception {
        final String sql = createQueryExpressionBodyScalar().except(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = createQueryExpressionBodyScalar().exceptAll(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = createQueryExpressionBodyScalar().exceptDistinct(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2 EXCEPT DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = createQueryExpressionBodyScalar().intersect(person.id).showQuery(new GenericDialect());
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = createQueryExpressionBodyScalar().intersectAll(person.id).showQuery(new GenericDialect());
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT ALL SELECT T0.id AS C0 FROM person AS T0", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = createQueryExpressionBodyScalar().intersectDistinct(person.id).showQuery(new GenericDialect());
        assertSimilar("(SELECT T1.id AS C0 FROM employee AS T1 UNION SELECT T2.id AS C0 FROM manager AS T2) INTERSECT DISTINCT SELECT T0.id AS C0 FROM person AS T0", sql);

    }

    public void testExists() throws Exception {
        final String sql = person.name.where(employee.id.where(employee.id.eq(person.id)).union(manager.id.where(manager.id.eq(person.id))).exists()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.id = T0.id UNION SELECT T2.id FROM manager AS T2 WHERE T2.id = T0.id)", sql);

    }

    public void testAsInSublist() throws Exception {
        final String sql = person.name.where(person.id.in(employee.id.except(manager.id))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE T0.id IN(SELECT T1.id FROM employee AS T1 EXCEPT SELECT T2.id FROM manager AS T2)", sql);
    }

    public void testContains() throws Exception {
        final String sql = person.name.where(employee.id.except(manager.id).contains(1L)).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE ? IN(SELECT T1.id FROM employee AS T1 EXCEPT SELECT T2.id FROM manager AS T2)", sql);
    }



    public void testList() throws Exception {
        final AbstractQueryExpressionBodyScalar<Long> queryExpressionBodyScalar = createQueryExpressionBodyScalar();
        new Scenario(queryExpressionBodyScalar) {
            @Override
            void use(AbstractQueryExpressionBodyScalar<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(Arrays.asList(123L), query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(createQueryExpressionBodyScalar()) {
            @Override
            void use(AbstractQueryExpressionBodyScalar<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Long>(123L)));
           }
        }.play();
    }

    public void testCompile() throws Exception {
        new Scenario(createQueryExpressionBodyScalar()) {
            @Override
            void use(AbstractQueryExpressionBodyScalar<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.compileQuery(engine).scroll(new TestCallback<Long>(123L)));
           }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<Long, AbstractQueryExpressionBodyScalar<Long>> {
        protected Scenario(AbstractQueryExpressionBodyScalar<Long> query) {
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
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

    private static class Employee extends TableOrView {
        @Override
        public String getTableName() {
            return "employee";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

    private static class Manager extends TableOrView {
        @Override
        public String getTableName() {
            return "manager";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

    private static Manager manager = new Manager();

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

    private DynamicParameter<Long> two = DynamicParameter.create(CoreMappers.LONG, 2L);

}
