package org.simqle.sql;

/**
 * @author lvovich
 */
public class SelectSublistTest extends SqlTestCase {

    public void testAll() throws Exception {
        String sql = person.id.select().all().show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDistinct() throws Exception {
        String sql = person.id.select().distinct().show();
        assertSimilar("SELECT DISTINCT T0.id AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(employee.id.select().booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(employee.id.select().in(manager.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IN(SELECT ALL T2.id FROM manager AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(employee.id.select().notIn(manager.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) NOT IN(SELECT ALL T2.id FROM manager AS T2)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.name.where(employee.id.select().in(person.id)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IN(T0.id)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.name.where(employee.id.select().notIn(person.id)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) NOT IN(T0.id)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.name.where(employee.id.select().isNull()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.name.where(employee.id.select().isNotNull()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.name.where(employee.id.select().eq(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) = ?", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.name.where(employee.id.select().ne(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) <> ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.name.where(employee.id.select().lt(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) < ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.name.where(employee.id.select().le(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) <= ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.name.where(employee.id.select().gt(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) > ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.name.where(employee.id.select().ge(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) >= ?", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = employee.id.select().opposite().orderBy(person.name).show();
        assertSimilar("SELECT -(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testPlus() throws Exception {
        final String sql = employee.id.select().plus(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) + T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMinus() throws Exception {
        final String sql = employee.id.select().minus(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) - T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMult() throws Exception {
        final String sql = employee.id.select().mult(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) * T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testDiv() throws Exception {
        final String sql = employee.id.select().div(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) / T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }


    public void testPlusNumber() throws Exception {
        final String sql = employee.id.select().plus(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) + ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMinusNumber() throws Exception {
        final String sql = employee.id.select().minus(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) - ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = employee.id.select().mult(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) * ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = employee.id.select().div(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) / ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testConcat() throws Exception {
        final String sql = employee.name.select().concat(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.name FROM employee AS T3) || T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = employee.name.select().concat(" test").orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.name FROM employee AS T3) || ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testSort() throws Exception {
        final String sql = person.name.orderBy(employee.name.select()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1)", sql);
    }

    public void testSortAsc() throws Exception {
        final String sql = person.name.orderBy(employee.name.select().asc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1) ASC", sql);
    }

    public void testSortDesc() throws Exception {
        final String sql = person.name.orderBy(employee.name.select().desc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1) DESC", sql);
    }

    public void testSortNullsFirst() throws Exception {
        final String sql = person.name.orderBy(employee.name.select().nullsFirst()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1) NULLS FIRST", sql);
    }

    public void testSortNullsLast() throws Exception {
        final String sql = person.name.orderBy(employee.name.select().nullsLast()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1) NULLS LAST", sql);
    }


    public void testWhere() throws Exception {
        final String sql = person.id.select().where(person.name.isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.id.select().orderBy(person.name).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testSelectSelect() throws Exception {
        final String sql = person.id.select().select().where(employee.name.isNotNull()).show();
        assertSimilar("SELECT(SELECT T0.id FROM person AS T0) AS C0 FROM employee AS T1 WHERE T1.name IS NOT NULL", sql);
    }







    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
    }

    private static Person person = new Person();

    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> retired = new LongColumn("retired", this);
    }

    private static Employee employee = new Employee();

    private static class Manager extends Table {
        private Manager() {
            super("manager");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> retired = new LongColumn("retired", this);
    }
    private static Manager manager = new Manager();

    private DynamicParameter<Long> two = new LongParameter(2L);

}
