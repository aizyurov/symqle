package org.simqle.coretest;

import org.simqle.sql.Column;
import org.simqle.sql.Table;


/**
 * @author lvovich
 */
public class BooleanFactorTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().and(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().or(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(NOT T0.alive)", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT NULL", sql);
    }


    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).and(person.friendly.booleanValue().negate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND NOT T0.friendly", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).or(person.friendly.booleanValue().negate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR NOT T0.friendly", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().eq(person.smart.booleanValue().negate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) =(NOT T0.smart)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().ne(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().gt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().ge(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().lt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) <(T0.smart)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().le(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) <=(T0.smart)", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().eq(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().ne(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().gt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().ge(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().lt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate().le(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) <= ?", sql);
    }


    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().negate().in(person2.alive.booleanValue().negate().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(NOT T1.smart) IN(SELECT NOT T2.alive FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().negate().notIn(person2.alive.booleanValue().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(NOT T1.smart) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().negate().in(person.alive.booleanValue().negate().asValue(), person.friendly.booleanValue().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.smart) IN(NOT T0.alive, T0.friendly)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().negate().notIn(person.alive.booleanValue().asValue(), person.friendly.booleanValue().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.smart) NOT IN(T0.alive, T0.friendly)", sql);
   }



    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<Long> alive = new LongColumn("alive", this);
        public Column<Long> smart = new LongColumn("smart", this);
        public Column<Long> cute = new LongColumn("cute", this);
        public Column<Long> speaksJapan = new LongColumn("speaks_japan", this);
        public Column<Long> friendly = new LongColumn("friendly", this);
    }

    private static Person person = new Person();
    private static Person person2 = new Person();


}
