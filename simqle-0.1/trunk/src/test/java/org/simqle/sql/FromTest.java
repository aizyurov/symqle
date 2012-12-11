package org.simqle.sql;

import junit.framework.TestCase;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 23.11.12
 * Time: 18:34
 * To change this template use File | Settings | File Templates.
 */
public class FromTest extends TestCase {

    public void testSimpleJoin() {
        final Person person = new Person();
//        final String sql = person.name().where(person.dept().deptName().isNotNull()).show();
        final String sql = person.name().show();
        assertEquals("SELECT T0.name AS C0 FROM person AS T0 LEFT OUTER JOIN department AS T1 ON T1.DEPT_ID = T0.dept_id WHERE T1.dept_name IS NOT NULL", sql);
    }

    public void testTableEquals() {
        final Person person1 = new Person();
        final Person person2 = new Person();
        assertFalse(person1.equals(person2));

        final Department dept11 = person1.dept();
        final Department dept12 = person1.dept();
        final Department dept21 = person2.dept();
        assertEquals(dept11,  dept12);
        assertFalse(dept11.equals(dept21));


    }

//    public void testDoubleJoin() {
//        // dept should be joined only once
//        final Person person = new Person();
//        final String sql = person.dept().deptId().where(person.dept().deptName().isNotNull()).show();
//        System.out.println(sql);
//
//    }

    private static class Person extends Table {
        private Person() {
            super("person");
        }

        public Column<Long> id() {
            return new LongColumn("id", this);
        }

        public Column<String> name() {
            return new StringColumn("name", this);
        }

        public Department dept() {
            final Department dept = new Department();
//            dept.leftJoin(dept.deptId().eq(deptId()));
            return dept;
        }

        public Column<Long> deptId() {
            return new LongColumn("dept_id", this);
        }
    }



    private static class Department extends Table {
        private Department() {
            super("department");
        }

        public Column<Long> deptId() {
            return new LongColumn("DEPT_ID", this);
        }

        public Column<String> deptName() {
            return new StringColumn("dept_name", this);
        }
    }
}
