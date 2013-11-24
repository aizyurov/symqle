package org.symqle.coretest;

import org.symqle.common.MalformedStatementException;
import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Table;
import org.symqle.sql.TableOrView;


/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 02.01.2013
 * Time: 21:30:08
 * To change this template use File | Settings | File Templates.
 */
public class JoinTest extends SqlTestCase {

    public void testChain() throws Exception {
        Person person = new Person();
        Person manager = new Person();
        Department department = new Department();
        person.leftJoin(manager, person.managerId.eq(manager.id));
        manager.leftJoin(department, manager.departmentId.eq(department.id));
        final String sql = person.name.concat(" ").concat(manager.name).concat(" ").concat(department.name).show(new GenericDialect());
        assertSimilar("SELECT T0.name || ? || T1.name || ? || T2.name AS C0 FROM person AS T0 LEFT JOIN person AS T1 LEFT JOIN department AS T2 ON T1.department_id = T2.id ON T0.manager_id = T1.id", sql);
    }

    public void testChainLimitedSelect() throws Exception {
        Person person = new Person();
        Person manager = new Person();
        Department department = new Department();
        person.leftJoin(manager, person.managerId.eq(manager.id));
        manager.leftJoin(department, manager.departmentId.eq(department.id));
        final String sql = manager.name.concat(" ").concat(department.name).show(new GenericDialect());
        assertSimilar("SELECT T1.name || ? || T2.name AS C0 FROM person AS T0 LEFT JOIN person AS T1 LEFT JOIN department AS T2 ON T1.department_id = T2.id ON T0.manager_id = T1.id", sql);
    }

    public void testTree() throws Exception {
        Person person = new Person();
        Person manager = new Person();
        Department department = new Department();
        person.leftJoin(manager, person.managerId.eq(manager.id));
        person.leftJoin(department, person.departmentId.eq(department.id));
        final String sql = person.name.concat(" ").concat(manager.name).concat(" ").concat(department.name).show(new GenericDialect());
        assertSimilar("SELECT T0.name || ? || T1.name || ? || T2.name AS C0 FROM person AS T0 LEFT JOIN person AS T1 ON T0.manager_id = T1.id LEFT JOIN department AS T2 ON T0.department_id = T2.id", sql);
    }

    public void testDoubleJoin() throws Exception {
        Person person = new Person();
        Person manager = new Person();
        Department department = new Department();
        person.leftJoin(department, person.departmentId.eq(department.id));
        try {
            manager.leftJoin(department, manager.departmentId.eq(manager.id));
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertTrue((e.getMessage().contains("already joined")));
        }
    }

    public void testDoubleSameTableJoin() throws Exception {
        Person person = new Person();
        Person manager = new Person();
        Department department = new Department();
        person.leftJoin(department, person.departmentId.eq(department.id));
        try {
            person.leftJoin(department, person.departmentId.eq(department.id));
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertTrue((e.getMessage().contains("already joined")));
        }
    }

    public void testCyclicJoin() throws Exception {
        Person person = new Person();
        Person manager = new Person();
        Department department = new Department();
        person.leftJoin(department, person.departmentId.eq(department.id));
        try {
            department.leftJoin(person, person.departmentId.eq(department.id));
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Cyclic join"));
        }
    }

    public void testSubqueryToParentJoin() throws Exception {
        Person person = new Person();
        Department department = new Department();
        Person manager = new Person();
        department.innerJoin(manager, department.managerId.eq(manager.id));
        final String sql = person.id.where(person.name.eq("John").and(person.managerId.eq(manager.id)))
            .queryValue().where(department.name.like("T%")).show(new GenericDialect());
        assertSimilar("SELECT(SELECT T6.id FROM person AS T6 WHERE T6.name = ? AND T6.manager_id = T5.id) AS C1 FROM department AS T4 INNER JOIN person AS T5 ON T4.manager_id = T5.id WHERE T4.name LIKE ?", sql);
    }

    public void testInsertOfJoinedTablesFails() {
        Person person = new Person();
        Person manager = new Person();
        Department department = new Department();
        person.leftJoin(department, person.departmentId.eq(department.id));
        try {
            final String sql = person.insert(department.name.set("John")).show(new GenericDialect());
            fail("MalformedStatementException expected but returned " + sql);
        } catch (MalformedStatementException e) {
            // Ok
        }
    }

    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> managerId = defineColumn(Mappers.LONG, "manager_id");
        public Column<Long> departmentId = defineColumn(Mappers.LONG, "department_id");
    }

    private static class Department extends Table {
        private Department() {
            super("department");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> managerId = defineColumn(Mappers.LONG, "manager_id");
    }


}
