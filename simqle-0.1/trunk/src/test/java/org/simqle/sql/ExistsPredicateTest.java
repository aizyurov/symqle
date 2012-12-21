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

    public void testSelectDistinct() throws Exception {
        try {
            final String sql = two.exists().distinct().show();
            fail ("IllegalStateException expected but produced: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("Generic dialect does not support selects with no tables", e.getMessage());
        }
    }

    public void testPredicate() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)", sql);
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().and(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) AND T0.id IS NOT NULL", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().or(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) OR T0.id IS NOT NULL", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) IS NOT NULL", sql);
    }

    public void testWhere() throws Exception {
        final String sql = employee.id.where(employee.name.eq(person.name)).exists().where(person.name.isNotNull()).show();
        assertSimilar("SELECT EXISTS(SELECT T0.id FROM employee AS T0 WHERE T0.name = T1.name) AS C0 FROM person AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().eq(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) =(T0.married)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().ne(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) <>(T0.married)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().gt(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) >(T0.married)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().ge(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) >=(T0.married)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().lt(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) <(T0.married)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().le(person.married.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) <=(T0.married)", sql);
    }

    public void testExcept() throws Exception {
        final String sql = manager.id.where(employee.name.eq(manager.name)).exists().except(manager.retired.booleanValue()).where(person.name.isNotNull()).show();
        assertSimilar("SELECT EXISTS(SELECT T0.id FROM employee AS T0 WHERE T0.name = T1.name) AS C0 FROM person AS T1 WHERE T1.name IS NOT NULL", sql);
    }





    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> married = new LongColumn("married", this);
    }

    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> retired = new LongColumn("retired", this);
    }

    private static class Manager extends Table {
        private Manager() {
            super("manager");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> retired = new LongColumn("retired", this);
    }

    private static Person person = new Person();
    private static Employee employee = new Employee();
    private static Manager manager = new Manager();

    private DynamicParameter<Long> two = new LongParameter(2L);




}
