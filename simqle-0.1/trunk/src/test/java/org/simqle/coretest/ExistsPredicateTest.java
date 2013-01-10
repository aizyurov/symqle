package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.Table;


/**
 * @author lvovich
 */
public class ExistsPredicateTest extends SqlTestCase {


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

    public void testIn() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().in(employee2.id.isNotNull().asValue().where(employee2.name.eq(person.name)))).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) IN(SELECT T2.id IS NOT NULL FROM employee AS T2 WHERE T2.name = T0.name)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().in(person.name.isNotNull().asValue(), person.name.isNull().asValue())).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) IN(T0.name IS NOT NULL, T0.name IS NULL)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().notIn(employee2.id.isNotNull().asValue().where(employee2.name.eq(person.name)))).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) NOT IN(SELECT T2.id IS NOT NULL FROM employee AS T2 WHERE T2.name = T0.name)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().notIn(person.name.isNotNull().asValue(), person.name.isNull().asValue())).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) NOT IN(T0.name IS NOT NULL, T0.name IS NULL)", sql);
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

    public void testEqValue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().eq(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().ne(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().gt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().ge(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().lt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().le(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) <= ?", sql);
    }

    public void testAsValue() throws Exception {
        final String sql = employee.id.where(employee.name.eq(person.name)).exists().asValue().orderBy(person.name).show();
        assertSimilar("SELECT EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) AS C0 FROM person AS T0 ORDER BY T0.name", sql);
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
    private static Employee employee2 = new Employee();
    private static Manager manager = new Manager();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);




}
