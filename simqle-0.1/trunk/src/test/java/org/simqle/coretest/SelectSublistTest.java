package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.Table;


/**
 * @author lvovich
 */
public class SelectSublistTest extends SqlTestCase {

    public void testAll() throws Exception {
        String sql = person.id.all().show();
        assertSimilar("SELECT ALL T0.id AS C0 FROM person AS T0", sql);
    }

    public void testDistinct() throws Exception {
        String sql = person.id.distinct().show();
        assertSimilar("SELECT DISTINCT T0.id AS C0 FROM person AS T0", sql);
    }

    public void testBooleanValue() throws Exception {
        final String sql = person.id.where(employee.id.queryValue().booleanValue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1)", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(employee.id.queryValue().in(manager.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IN(SELECT ALL T2.id FROM manager AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(employee.id.queryValue().notIn(manager.id.all())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) NOT IN(SELECT ALL T2.id FROM manager AS T2)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().in(person.id, person.id.opposite())).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IN(T0.id, - T0.id)", sql);
    }

    public void testNotInList() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().notIn(person.id, person.id.opposite())).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) NOT IN(T0.id, - T0.id)", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().isNull()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().isNotNull()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().eq(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) = ?", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().ne(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) <> ?", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().lt(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) < ?", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().le(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) <= ?", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().gt(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) > ?", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.name.where(employee.id.queryValue().ge(two)).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 WHERE(SELECT T1.id FROM employee AS T1) >= ?", sql);
    }

    public void testOpposite() throws Exception {
        final String sql = employee.id.queryValue().opposite().orderBy(person.name).show();
        assertSimilar("SELECT -(SELECT T3.id FROM employee AS T3) AS C1 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testPlus() throws Exception {
        final String sql = employee.id.queryValue().plus(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) + T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testPair() throws Exception {
        final String sql = employee.id.queryValue().pair(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) AS C0, T2.id AS C1 FROM person AS T2 ORDER BY T2.name", sql);
    }
    public void testMinus() throws Exception {
        final String sql = employee.id.queryValue().minus(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) - T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMult() throws Exception {
        final String sql = employee.id.queryValue().mult(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) * T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testDiv() throws Exception {
        final String sql = employee.id.queryValue().div(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) / T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }


    public void testPlusNumber() throws Exception {
        final String sql = employee.id.queryValue().plus(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) + ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMinusNumber() throws Exception {
        final String sql = employee.id.queryValue().minus(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) - ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testMultNumber() throws Exception {
        final String sql = employee.id.queryValue().mult(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) * ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testDivNumber() throws Exception {
        final String sql = employee.id.queryValue().div(2).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.id FROM employee AS T3) / ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testConcat() throws Exception {
        final String sql = employee.name.queryValue().concat(person.id).orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.name FROM employee AS T3) || T2.id AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testConcatString() throws Exception {
        final String sql = employee.name.queryValue().concat(" test").orderBy(person.name).show();
        assertSimilar("SELECT(SELECT T3.name FROM employee AS T3) || ? AS C0 FROM person AS T2 ORDER BY T2.name", sql);
    }

    public void testSort() throws Exception {
        final String sql = person.name.orderBy(employee.name.queryValue()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1)", sql);
    }

    public void testSortAsc() throws Exception {
        final String sql = person.name.orderBy(employee.name.queryValue().asc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1) ASC", sql);
    }

    public void testSortDesc() throws Exception {
        final String sql = person.name.orderBy(employee.name.queryValue().desc()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1) DESC", sql);
    }

    public void testSortNullsFirst() throws Exception {
        final String sql = person.name.orderBy(employee.name.queryValue().nullsFirst()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1) NULLS FIRST", sql);
    }

    public void testSortNullsLast() throws Exception {
        final String sql = person.name.orderBy(employee.name.queryValue().nullsLast()).show();
        assertSimilar("SELECT T0.name AS C0 FROM person AS T0 ORDER BY(SELECT T1.name FROM employee AS T1) NULLS LAST", sql);
    }


    public void testWhere() throws Exception {
        final String sql = person.id.where(person.name.isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name IS NOT NULL", sql);
    }

    public void testOrderBy() throws Exception {
        final String sql = person.id.orderBy(person.name).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.name", sql);
    }

    public void testSelectSelect() throws Exception {
        final String sql = person.id.queryValue().where(employee.name.isNotNull()).show();
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

    private DynamicParameter<Long> two = DynamicParameter.create(Mappers.LONG, 2L);

}
