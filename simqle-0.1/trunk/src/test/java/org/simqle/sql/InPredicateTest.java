package org.simqle.sql;

/**
 * @author lvovich
 */
public class InPredicateTest extends SqlTestCase {

    public void testSelect() throws Exception {
        final String sql = person.id.in(employee.id).show();
        assertSimilar("SELECT T1.id IN(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T1", sql);
    }

    public void testAll() throws Exception {
        final String sql = person.id.in(employee.id).all().show();
        assertSimilar("SELECT ALL T1.id IN(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T1", sql);
    }

    public void testDistinct() throws Exception {
        final String sql = person.id.in(employee.id).distinct().show();
        assertSimilar("SELECT DISTINCT T1.id IN(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T1", sql);
    }

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

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1))", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).in(employee.name.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) IN(SELECT T2.name IS NOT NULL FROM employee AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).notIn(employee.name.isNotNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) NOT IN(SELECT T2.name IS NOT NULL FROM employee AS T2)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).in(person.name.isNotNull(), person.name.isNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name IN(SELECT T1.name FROM employee AS T1)) IN(T0.name IS NOT NULL, T0.name IS NULL)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(person.name.in(employee.name).notIn(person.name.isNotNull(), person.name.isNull())).show();
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

    public void testWhere() throws Exception {
        final String sql = person.name.in(employee.name).where(person.name.isNotNull()).show();
        assertSimilar("SELECT T1.name IN(SELECT T3.name FROM employee AS T3) AS C1 FROM person AS T1 WHERE T1.name IS NOT NULL", sql);
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

    public void testPlus() throws Exception {
        final String sql = person.id.in(employee.id).plus(two).show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) + ? AS C1 FROM person AS T1", sql);

    }

    public void testMinus() throws Exception {
        final String sql = person.id.in(employee.id).minus(two).show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) - ? AS C1 FROM person AS T1", sql);
    }

    public void testMult() throws Exception {
        final String sql = person.id.in(employee.id).mult(two).show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) * ? AS C1 FROM person AS T1", sql);
    }

    public void testDiv() throws Exception {
        final String sql = person.id.in(employee.id).div(two).show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) / ? AS C1 FROM person AS T1", sql);
    }


    public void testPlusNumber() throws Exception {
        final String sql = person.id.in(employee.id).plus(2).show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) + ? AS C1 FROM person AS T1", sql);

    }

    public void testMinusNumber() throws Exception {
        final String sql = person.id.in(employee.id).minus(2).show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) - ? AS C1 FROM person AS T1", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = person.id.in(employee.id).mult(2).show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) * ? AS C1 FROM person AS T1", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = person.id.in(employee.id).div(2).show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) / ? AS C1 FROM person AS T1", sql);
    }

    public void testConcat() throws Exception {
        final String sql = person.id.in(employee.id).concat(person.name).show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) || T1.name AS C1 FROM person AS T1", sql);

    }

    public void testConcatString() throws Exception {
        final String sql = person.id.in(employee.id).concat(" test").show();
        assertSimilar("SELECT(T1.id IN(SELECT T3.id FROM employee AS T3)) || ? AS C1 FROM person AS T1", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = person.id.in(employee.id).opposite().show();
        assertSimilar("SELECT -(T1.id IN(SELECT T3.id FROM employee AS T3)) AS C1 FROM person AS T1", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.id.in(employee.id).orderBy(person.name).show();
        assertSimilar("SELECT T1.id IN(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T1 ORDER BY T1.name", sql);
    }

    public void testSort() throws Exception {
        final String sql = person.name.orderBy(person.id.in(employee.id)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.id IN(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testSortAsc() throws Exception {
        final String sql = person.name.orderBy(person.id.in(employee.id).asc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.id IN(SELECT T1.id FROM employee AS T1) ASC", sql);
    }

    public void testSortDesc() throws Exception {
        final String sql = person.name.orderBy(person.id.in(employee.id).desc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.id IN(SELECT T1.id FROM employee AS T1) DESC", sql);
    }

    public void testSortNullsFirst() throws Exception {
        final String sql = person.name.orderBy(person.id.in(employee.id).nullsFirst()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY T0.id IN(SELECT T1.id FROM employee AS T1) NULLS FIRST", sql);
    }

    public void testSortNullsLast() throws Exception {
        final String sql = person.name.orderBy(person.id.in(employee.id).nullsLast()).show();
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
