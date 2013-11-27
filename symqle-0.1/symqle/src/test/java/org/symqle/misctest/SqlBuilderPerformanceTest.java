package org.symqle.misctest;

import junit.framework.TestCase;
import org.symqle.common.Mappers;
import org.symqle.dialect.FastGenericDialect;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class SqlBuilderPerformanceTest extends TestCase {

    public void testGeneric() throws Exception {
        Person person = new Person();
        Department department = new Department();
        Person manager = new Person();
        department.innerJoin(manager, department.managerId.eq(manager.id));
        {
            final String sql = person.id.where(person.name.eq("John").and(person.managerId.eq(manager.id)))
                .queryValue().where(department.name.like("T%")).show(new GenericDialect());
        }
        {
            final long start = System.nanoTime();
            final String sql = person.id.where(person.name.eq("John").and(person.managerId.eq(manager.id)))
                .queryValue().where(department.name.like("T%")).show(new GenericDialect());
            System.out.println("Time: " + (System.nanoTime() - start) /1000 + " micros");
        }

    }

    public void testFastGeneric() throws Exception {
        Person person = new Person();
        Department department = new Department();
        Person manager = new Person();
        department.innerJoin(manager, department.managerId.eq(manager.id));
        {
            final String sql = person.id.where(person.name.eq("John").and(person.managerId.eq(manager.id)))
                .queryValue().where(department.name.like("T%")).show(new FastGenericDialect());
        }
        {
            final long start = System.nanoTime();
            final String sql = person.id.where(person.name.eq("John").and(person.managerId.eq(manager.id)))
                .queryValue().where(department.name.like("T%")).show(new FastGenericDialect());
            System.out.println("Time: " + (System.nanoTime() - start) /1000 + " micros");
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

    public static void main(String[] args) {
        final Integer limit = Integer.valueOf(args[0]);
        System.out.println("Starting " + limit + " iterations");
        Person person = new Person();
        Department department = new Department();
        Person manager = new Person();
        department.innerJoin(manager, department.managerId.eq(manager.id));
        final long start = System.nanoTime();
        for (int i=0; i< limit; i++)
        {
            final String sql = person.id.where(person.name.eq("John").and(person.managerId.eq(manager.id)))
                .queryValue().where(department.name.like("T%")).show(new GenericDialect());
        }
        System.out.println("Average time: " + (System.nanoTime() - start) / limit / 1000 + "micros");


    }

}
