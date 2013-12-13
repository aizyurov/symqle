package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;


/**
 * @author lvovich
 */
public class ExistsPredicateTest extends SqlTestCase {


    public void testPredicate() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)", sql);
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().and(person.id.isNotNull())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) AND T0.id IS NOT NULL", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().or(person.id.isNotNull())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) OR T0.id IS NOT NULL", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().negate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isFalse()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotFalse()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isUnknown()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().isNotUnknown()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) IS NOT UNKNOWN", sql);
    }

    public void testAsValue() throws Exception {
        final String sql = employee.id.where(employee.name.eq(person.name)).exists().asValue().orderBy(person.name).show(new GenericDialect());
        assertSimilar("SELECT EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) AS C0 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testThen() throws Exception {
        final String sql = employee.id.where(employee.name.eq(person.name)).exists().then(person.name).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) THEN T0.name END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.name.isNotNull().then(person.name).orWhen(employee.id.where(employee.name.eq(person.name)).exists().thenNull()).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NOT NULL THEN T0.name WHEN EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) THEN NULL END AS C0 FROM person AS T0", sql);
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Boolean> married = defineColumn(Mappers.BOOLEAN, "married");
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> retired = defineColumn(Mappers.LONG, "retired");
    }

    private static Person person = new Person();
    private static Employee employee = new Employee();
    private static Employee employee2 = new Employee();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);




}
