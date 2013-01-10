package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.TableOrView;


/**
 * @author lvovich
 */
public class InPredicateTest extends SqlTestCase {

    public void testPredicate() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1)", sql);
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).and(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) AND T0.id IS NOT NULL", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).or(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) OR T0.id IS NOT NULL", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.name IN(SELECT T1.name FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).in(employee.name.isNotNull().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) IN(SELECT T2.name IS NOT NULL FROM employee AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).notIn(employee.name.isNotNull().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) NOT IN(SELECT T2.name IS NOT NULL FROM employee AS T2)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).in(person.name.isNotNull().asValue(), person.name.isNull().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) IN(T0.name IS NOT NULL, T0.name IS NULL)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).notIn(person.name.isNotNull().asValue(), person.name.isNull().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) NOT IN(T0.name IS NOT NULL, T0.name IS NULL)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).eq(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) =(T0.id IS NOT NULL)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).ne(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) <>(T0.id IS NOT NULL)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).gt(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) >(T0.id IS NOT NULL)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).ge(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) >=(T0.id IS NOT NULL)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).lt(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) <(T0.id IS NOT NULL)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).le(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) <=(T0.id IS NOT NULL)", sql);
    }


    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).eq(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).ne(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).gt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).ge(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).lt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).le(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) <= ?", sql);
    }

    public void testAsValue() throws Exception {
        final String sql = person.name.in(employee.name).asValue().show();
        assertSimilar("SELECT T0.name IN(SELECT T1.name FROM employee AS T1) AS C0 FROM person AS T0", sql);
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }


    private static Person person = new Person();
    private static Employee employee = new Employee();

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
