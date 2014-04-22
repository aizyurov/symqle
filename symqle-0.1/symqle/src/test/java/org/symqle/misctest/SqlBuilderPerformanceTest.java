package org.symqle.misctest;

import junit.framework.TestCase;
import org.symqle.common.CoreMappers;
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
            for (int k=0; k < 10; k++) {
                final long start = System.nanoTime();
                for (int i=0; i< 10000; i++) {
                final String sql = person.id.where(person.name.eq("John").and(person.managerId.eq(manager.id)))
                    .queryValue().where(department.name.like("T%")).showQuery(new GenericDialect());
                }
                System.out.println("Time: " + (System.nanoTime() - start) /1000/10000 + " micros");
            }
        }

    }

    private static class Person extends Table {
        @Override
        public String getTableName() {
            return "person";
        }

        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<Long> managerId = defineColumn(CoreMappers.LONG, "manager_id");
        public Column<Long> departmentId = defineColumn(CoreMappers.LONG, "department_id");
    }

    private static class Department extends Table {
        @Override
        public String getTableName() {
            return "department";
        }

        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<Long> managerId = defineColumn(CoreMappers.LONG, "manager_id");
    }

}
