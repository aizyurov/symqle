package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.MalformedStatementException;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Label;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class LabelTest extends SqlTestCase {

    public void testPlainUsage() throws Exception {
        Label l = new Label();
        final String sql = person.name.label(l).orderBy(l).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY C0", sql);
    }

    public void testSubquery() throws Exception {
        Label l = new Label();
        try {
            final String sql = person.id.where(person.name.in(employee.name.label(l))).orderBy(l).showQuery(new GenericDialect());
            fail("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // fine
        }
    }

    public void testUnion() throws Exception {
        Label l = new Label();
        final String sql = person.name.label(l).union(employee.name).orderBy(l).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 UNION SELECT T0.name AS C0 FROM employee AS T0 ORDER BY C0", sql);
    }

    public void testUnassigned() throws Exception {
        Label l = new Label();
        try {
            final String sql = person.id.orderBy(l).showQuery(new GenericDialect());
            fail("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // fine
        }
    }

    public void testPair() throws Exception {
        Label l1 = new Label();
        Label l2 = new Label();
        final String sql = person.id.label(l1).pair(person.name.label(l2)).orderBy(l2, l1).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 ORDER BY C1, C0", sql);
    }

    public void testDuplicate() throws Exception {
        Label l1 = new Label();
        try {
            final String sql = person.id.label(l1).pair(person.name.label(l1)).orderBy(l1).showQuery(new GenericDialect());
            fail("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // ok
        }
    }

    public void testAsc() throws Exception {
        Label l = new Label();
        final String sql = person.name.label(l).orderBy(l.asc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY C0 ASC", sql);
    }

    public void testDesc() throws Exception {
        Label l = new Label();
        final String sql = person.name.label(l).orderBy(l.desc()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY C0 DESC", sql);
    }

    public void testNullsFirst() throws Exception {
        Label l = new Label();
        final String sql = person.name.label(l).orderBy(l.nullsFirst()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY C0 NULLS FIRST", sql);
    }

    public void testNullsLast() throws Exception {
        Label l = new Label();
        final String sql = person.name.label(l).orderBy(l.nullsLast()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY C0 NULLS LAST", sql);
    }




    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<Long> age = defineColumn(CoreMappers.LONG, "age");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<Long> parentId = defineColumn(CoreMappers.LONG, "parent_id");
    }

    private static class Employee extends TableOrView {
        @Override
        public String getTableName() {
            return "employee";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static Employee employee = new Employee();

}
