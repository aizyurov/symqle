package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class QuerySpecificationTest extends SqlTestCase {


    public void testShow() throws Exception {
        final AbstractQuerySpecificationScalar<Long> querySpecification = person.id.where(person.name.isNotNull());
        final String sql = querySpecification.show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
        assertSimilar(sql, querySpecification.show(new GenericDialect()));
    }

    public void testQueryValue() throws Exception {
        final AbstractQuerySpecificationScalar<Long> specification = employee.id.where(employee.name.isNotNull());
        final String sql = specification.queryValue().orderBy(person.name).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3 WHERE T3.name IS NOT NULL) AS C1 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL FOR READ ONLY", sql);
    }

    public void testUnion() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).union(employee.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL UNION SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).unionAll(employee.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL UNION ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).unionDistinct(employee.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL UNION DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExcept() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).except(employee.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL EXCEPT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).exceptAll(employee.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL EXCEPT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).exceptDistinct(employee.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL EXCEPT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).intersect(employee.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL INTERSECT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).intersectAll(employee.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL INTERSECT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).intersectDistinct(employee.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL INTERSECT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExists() throws Exception {
        final String sql = employee.id.where(person.id.where(person.name.eq(employee.name)).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE EXISTS(SELECT T1.id FROM person AS T1 WHERE T1.name = T0.name)", sql);

    }

    public void testContains() throws Exception {
        final String sql = employee.id.where(person.id.where(person.name.eq(employee.name)).contains(1L)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM employee AS T0 WHERE ? IN(SELECT T1.id FROM person AS T1 WHERE T1.name = T0.name)", sql);
    }


    public void testList() throws Exception {
        new Scenario123<AbstractQuerySpecificationScalar<Long>>(person.id.where(person.name.isNull())) {
            @Override
            void use(AbstractQuerySpecificationScalar<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(getExpected(), query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario123<AbstractQuerySpecificationScalar<Long>>(person.id.where(person.name.isNull())) {
            @Override
            void use(AbstractQuerySpecificationScalar<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, getCallback()));
            }
        }.play();
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Employee employee = new Employee();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
