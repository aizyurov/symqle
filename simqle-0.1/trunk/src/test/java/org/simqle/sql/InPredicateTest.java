package org.simqle.sql;

/**
 * @author lvovich
 */
public class InPredicateTest extends SqlTestCase {

    public void testSelect() throws Exception {
        final String sql = person.id.in(employee.id.select()).select().show();
        assertSimilar("SELECT T1.id IN(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T1", sql);
    }

    public void testAll() throws Exception {
        final String sql = person.id.in(employee.id.select()).all().show();
        assertSimilar("SELECT ALL T1.id IN(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T1", sql);
    }

    public void testDistinct() throws Exception {
        final String sql = person.id.in(employee.id.select()).distinct().show();
        assertSimilar("SELECT DISTINCT T1.id IN(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T1", sql);
    }

    public void testPredicate() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1)", sql);
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).and(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) AND T0.id IS NOT NULL", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).or(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) OR T0.id IS NOT NULL", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.name IN(SELECT T1.name FROM employee AS T1)", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1))", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).in(employee.name.isNotNull().select())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) IN(SELECT T2.name IS NOT NULL FROM employee AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).notIn(employee.name.isNotNull().select())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) NOT IN(SELECT T2.name IS NOT NULL FROM employee AS T2)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).in(person.name.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) IN(T0.name IS NOT NULL)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).notIn(person.name.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) NOT IN(T0.name IS NOT NULL)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IN(SELECT T1.name FROM employee AS T1) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) IS NOT NULL", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.name.in(employee.name.select()).where(person.name.isNotNull()).show();
        assertSimilar("SELECT T1.name IN(SELECT T3.name FROM employee AS T3) AS C1 FROM person AS T1 WHERE T1.name IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).eq(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) =(T0.id IS NOT NULL)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).ne(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) <>(T0.id IS NOT NULL)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).gt(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) >(T0.id IS NOT NULL)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).ge(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) >=(T0.id IS NOT NULL)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).lt(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) <(T0.id IS NOT NULL)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name.select()).le(person.id.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) <=(T0.id IS NOT NULL)", sql);
    }

    public void testPlus() throws Exception {
        final String sql = person.id.in(employee.id.select()).plus(two).select().show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) + ? AS C1 FROM person AS T1", sql);

    }

    public void testMinus() throws Exception {
        final String sql = person.id.in(employee.id.select()).minus(two).select().show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) - ? AS C1 FROM person AS T1", sql);
    }

    public void testMult() throws Exception {
        final String sql = person.id.in(employee.id.select()).mult(two).select().show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) * ? AS C1 FROM person AS T1", sql);
    }

    public void testDiv() throws Exception {
        final String sql = person.id.in(employee.id.select()).div(two).select().show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) / ? AS C1 FROM person AS T1", sql);
    }


    public void testPlusNumber() throws Exception {
        final String sql = person.id.in(employee.id.select()).plus(2).select().show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) + ? AS C1 FROM person AS T1", sql);

    }

    public void testMinusNumber() throws Exception {
        final String sql = person.id.in(employee.id.select()).minus(2).select().show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) - ? AS C1 FROM person AS T1", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = person.id.in(employee.id.select()).mult(2).select().show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) * ? AS C1 FROM person AS T1", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = person.id.in(employee.id.select()).div(2).select().show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) / ? AS C1 FROM person AS T1", sql);
    }

    public void testConcat() throws Exception {
        final String sql = person.id.in(employee.id.select()).concat(person.name).select().show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) || T1.name AS C1 FROM person AS T1", sql);

    }

    public void testConcatString() throws Exception {
        final String sql = person.id.in(employee.id.select()).concat(" test").select().show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) || ? AS C1 FROM person AS T1", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = person.id.in(employee.id.select()).opposite().select().show();
        assertSimilar("SELECT -(T1.id IN(SELECT T3.id FROM employee AS T3)) AS C1 FROM person AS T1", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.id.in(employee.id.select()).select().orderBy(person.name).show();
        assertSimilar("SELECT T1.id IN(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T1 ORDER BY T1.name", sql);
    }

    public void testSort() throws Exception {
        final String sql = person.name.select().orderBy(person.id.in(employee.id.select())).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.id IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testSortAsc() throws Exception {
        final String sql = person.name.select().orderBy(person.id.in(employee.id.select()).asc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.id IN(SELECT T1.id FROM employee AS T1) ASC", sql);
    }

    public void testSortDesc() throws Exception {
        final String sql = person.name.select().orderBy(person.id.in(employee.id.select()).desc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.id IN(SELECT T1.id FROM employee AS T1) DESC", sql);
    }

    public void testSortNullsFirst() throws Exception {
        final String sql = person.name.select().orderBy(person.id.in(employee.id.select()).nullsFirst()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.id IN(SELECT T1.id FROM employee AS T1) NULLS FIRST", sql);
    }

    public void testSortNullsLast() throws Exception {
        final String sql = person.name.select().orderBy(person.id.in(employee.id.select()).nullsLast()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.id IN(SELECT T1.id FROM employee AS T1) NULLS LAST", sql);
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

    private DynamicParameter<Long> two = new LongParameter(2L);

}
