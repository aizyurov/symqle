package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractQueryBaseScalar;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;

/**
 * @author lvovich
 */
public class QueryBaseScalarTest extends SqlTestCase {


    private AbstractQueryBaseScalar<Long> createQueryBaseScalar() {
        return person.id.selectAll();
    }

    public void testShow() throws Exception {
        final AbstractQueryBaseScalar<Long> qbs = createQueryBaseScalar();
        final String sql = qbs.show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0", sql);
        final String sql2 = qbs.show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testAdapt() throws Exception {
        final AbstractQueryBaseScalar<Long> adaptor = AbstractQueryBaseScalar.adapt(person.id);
        assertEquals(person.id.getMapper(), adaptor.getMapper());
        assertEquals(person.id.show(new GenericDialect()), adaptor.show(new GenericDialect()));
    }

    public void testLimit() throws Exception {
        final String sql = createQueryBaseScalar().limit(10).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 FETCH FIRST 10 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final String sql = createQueryBaseScalar().limit(10,20).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }


    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(employee.id.selectAll().queryValue().asPredicate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(employee.id.selectAll().queryValue().in(manager.id.selectAll())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT ALL T1.id FROM employee AS T1) IN(SELECT ALL T2.id FROM manager AS T2)", sql);
    }

    public void testSort() throws Exception {
        final String sql = person.name.orderBy(employee.name.selectAll().queryValue()).show(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT ALL T1.name FROM employee AS T1)", sql);
    }


    public void testWhere() throws Exception {

        final String sql = createQueryBaseScalar().where(person.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT ALL T2.id AS C1 FROM person AS T2 WHERE T2.name IS NOT NULL", sql);
    }

    public void testAllForUpdate() throws Exception {
        final String sql = createQueryBaseScalar().forUpdate().show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testDistincgForUpdate() throws Exception {
        final String sql = person.id.distinct().forUpdate().show(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = createQueryBaseScalar().forUpdate().show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createQueryBaseScalar().forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testUnion() throws Exception {
        final String sql = createQueryBaseScalar().union(employee.id).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 UNION SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testUnionAll() throws Exception {
        final String sql = createQueryBaseScalar().unionAll(employee.id).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 UNION ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testUnionDistinct() throws Exception {
        final String sql = createQueryBaseScalar().unionDistinct(employee.id).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExcept() throws Exception {
        final String sql = createQueryBaseScalar().except(employee.id).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 EXCEPT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExceptAll() throws Exception {
        final String sql = createQueryBaseScalar().exceptAll(employee.id).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testExceptDistinct() throws Exception {
        final String sql = createQueryBaseScalar().exceptDistinct(employee.id).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersect() throws Exception {
        final String sql = createQueryBaseScalar().intersect(employee.id).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 INTERSECT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testIntersectAll() throws Exception {
        final String sql = createQueryBaseScalar().intersectAll(employee.id).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.id AS C0 FROM employee AS T1", sql);

    }
    public void testIntersectDistinct() throws Exception {
        final String sql = createQueryBaseScalar().intersectDistinct(employee.id).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.id AS C0 FROM employee AS T1", sql);

    }

    public void testExists() throws Exception {
        final String sql = createQueryBaseScalar().where(employee.name.selectAll().exists()).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT ALL T1.name FROM employee AS T1)", sql);
    }

    public void testContains() throws Exception {
        final String sql = createQueryBaseScalar().where(employee.name.selectAll().contains("Jim")).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 WHERE ? IN(SELECT ALL T1.name FROM employee AS T1)", sql);

    }

    public void testQueryValue() throws Exception {
        final String sql = createQueryBaseScalar().queryValue().where(employee.name.isNotNull()).show(new GenericDialect());
        assertSimilar("SELECT(SELECT ALL T0.id FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }


    public void testList() throws Exception {
        new Scenario123<AbstractQueryBaseScalar<Long>>(createQueryBaseScalar()) {
            @Override
            void use(AbstractQueryBaseScalar<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(getExpected(), query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario123<AbstractQueryBaseScalar<Long>>(createQueryBaseScalar()) {
            @Override
            void use(AbstractQueryBaseScalar<Long> query, QueryEngine engine) throws SQLException {
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

    private static class Manager extends TableOrView {
        private Manager() {
            super("manager");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }
    private static Manager manager = new Manager();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
