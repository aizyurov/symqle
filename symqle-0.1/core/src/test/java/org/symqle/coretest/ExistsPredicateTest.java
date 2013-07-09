package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.TableOrView;


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
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().in(true, false)).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) IN(?, ?)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().notIn(employee2.id.isNotNull().asValue().where(employee2.name.eq(person.name)))).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) NOT IN(SELECT T2.id IS NOT NULL FROM employee AS T2 WHERE T2.name = T0.name)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().notIn(true, false)).show();
        System.out.println(sql);
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) NOT IN(?, ?)", sql);
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
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().eq(person.married.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) =(T0.married)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().ne(person.married.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) <>(T0.married)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().gt(person.married.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) >(T0.married)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().ge(person.married.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) >=(T0.married)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().lt(person.married.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name)) <(T0.married)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(employee.id.where(employee.name.eq(person.name)).exists().le(person.married.asPredicate())).show();
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

    public void testThen() throws Exception {
        final String sql = employee.id.where(employee.name.eq(person.name)).exists().then(person.name).show();
        assertSimilar("SELECT CASE WHEN EXISTS(SELECT T1.id FROM employee AS T1 WHERE T1.name = T0.name) THEN T0.name END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.name.isNotNull().then(person.name).orWhen(employee.id.where(employee.name.eq(person.name)).exists().thenNull()).show();
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
