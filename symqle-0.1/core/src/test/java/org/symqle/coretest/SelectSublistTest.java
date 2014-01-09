package org.symqle.coretest;

import org.symqle.common.MalformedStatementException;
import org.symqle.common.Mappers;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractPredicate;
import org.symqle.sql.AbstractSelectSublist;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Label;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;
import java.util.Arrays;


/**
 * @author lvovich
 */
public class SelectSublistTest extends SqlTestCase {

    public void testShow() throws Exception {
        final Label l = new Label();
        String sql = person.id.label(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0", sql);
    }

    public void testAdapt() throws Exception {
        final AbstractSelectSublist<Long> adaptor = AbstractSelectSublist.adapt(person.id);
        final String sql = adaptor.show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0", sql);
        assertEquals(Mappers.LONG, adaptor.getMapper());
    }

    public void testSelectAll() throws Exception {
        final Label l = new Label();
        String sql = person.id.label(l).selectAll().orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0 ORDER BY C0", sql);
    }

    public void testDistinct() throws Exception {
        final Label l = new Label();
        String sql = person.id.label(l).distinct().orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT DISTINCT T0.id AS C0 FROM person AS T0 ORDER BY C0", sql);
    }

    public void testForUpdate() throws Exception {
        final Label l = new Label();
        String sql = person.id.label(l).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final Label l = new Label();
        String sql = person.id.label(l).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 FOR READ ONLY", sql);
    }

    public void testLimit() throws Exception {
        final Label l = new Label();
        final String sql = person.id.label(l).limit(20).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testLimit2() throws Exception {
        final Label l = new Label();
        final String sql = person.id.label(l).limit(10, 20).show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", sql);
    }

    public void testExists() throws Exception {
        final Label l = new Label();
        final AbstractPredicate predicate = employee.id.label(l).exists();
        try {
            final String sql = person.id.where(predicate).show(new GenericDialect());
            fail("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // ok
        }
    }

    public void testContains() throws Exception {
        final Label l = new Label();
        final AbstractPredicate predicate = employee.id.label(l).contains(1L);
        try {
            final String sql = person.id.where(predicate).show(new GenericDialect());
            fail("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // ok
        }
    }

    public void testWhere() throws Exception {
        final Label l = new Label();
        final String sql = person.id.label(l).where(person.name.isNotNull()).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL ORDER BY C0", sql);
    }

    public void testOrderBy() throws Exception {
        final Label l = new Label();
        final String sql = person.id.label(l).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY C0", sql);
    }
    public void testExceptAll() throws Exception {
        final Label l = new Label();
        final Column<Long> column = person.id;
        final String sql = column.label(l).exceptAll(person2.age).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.age AS C0 FROM person AS T1 ORDER BY C0", sql);
    }

    public void testExceptDistinct() throws Exception {
        final Label l = new Label();
        final Column<Long> column = person.id;
        final String sql = column.label(l).exceptDistinct(person.age).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.age AS C0 FROM person AS T1 ORDER BY C0", sql);
    }

    public void testExcept() throws Exception {
        final Label l = new Label();
        final Column<Long> column = person.id;
        final String sql = column.label(l).except(person2.age).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 EXCEPT SELECT T1.age AS C0 FROM person AS T1 ORDER BY C0", sql);
    }

    public void testUnionAll() throws Exception {
        final Label l = new Label();
        final Column<Long> column = person.id;
        final String sql = column.label(l).unionAll(person2.age).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION ALL SELECT T1.age AS C0 FROM person AS T1 ORDER BY C0", sql);
    }

    public void testUnionDistinct() throws Exception {
        final Label l = new Label();
        final Column<Long> column = person.id;
        final String sql = column.label(l).unionDistinct(person2.age).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.age AS C0 FROM person AS T1 ORDER BY C0", sql);
    }

    public void testUnion() throws Exception {
        final Label l = new Label();
        final Column<Long> column = person.id;
        final String sql = column.label(l).union(person2.age).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 UNION SELECT T1.age AS C0 FROM person AS T1 ORDER BY C0", sql);
    }

    public void testIntersectAll() throws Exception {
        final Label l = new Label();
        final Column<Long> column = person.id;
        final String sql = column.label(l).intersectAll(person2.age).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.age AS C0 FROM person AS T1 ORDER BY C0", sql);

    }

    public void testIntersectDistinct() throws Exception {
        final Label l = new Label();
        final Column<Long> column = person.id;
        final String sql = column.label(l).intersectDistinct(person2.age).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1 ORDER BY C0", sql);

    }

    public void testIntersect() throws Exception {
        final Label l = new Label();
        final Column<Long> column = person.id;
        final String sql = column.label(l).intersect(person2.age).orderBy(l).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 INTERSECT SELECT T1.age AS C0 FROM person AS T1 ORDER BY C0", sql);

    }

    public void testList() throws Exception {
        final Label l = new Label();
        final AbstractSelectSublist<Long> selectSublist = person.id.label(l);
        new Scenario123<AbstractSelectSublist<Long>>(selectSublist){
            @Override
            void use(final AbstractSelectSublist<Long> query, final QueryEngine engine) throws SQLException {
                assertEquals(Arrays.asList(123L), query.list(engine));
            }
        }.play();
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


    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
    }

    private static Person person = new Person();
    private static Person person2 = new Person();

    private static Employee employee = new Employee();

    private static Manager manager = new Manager();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
