package org.simqle.sql;

/**
 * @author lvovich
 */
public class ExistsPredicateTest extends SqlTestCase {

    public void testSelect() throws Exception {
        try {
            final String sql = two.exists().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testSelectAll() throws Exception {
        try {
            final String sql = two.exists().all().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
    }

    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
    }

    private static Person person = new Person();
    private static Employee employee = new Employee();

    private DynamicParameter<Long> two = new LongParameter(2L);




}
