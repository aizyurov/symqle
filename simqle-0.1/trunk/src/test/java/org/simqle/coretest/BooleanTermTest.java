package org.simqle.coretest;

import org.simqle.sql.Column;
import org.simqle.sql.Table;


/**
 * @author lvovich
 */
public class BooleanTermTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).and(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive AND T0.cute AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).or(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive AND T0.cute OR T0.smart", sql);
    }


    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(T0.alive AND T0.cute)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT NULL", sql);
    }


    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).and(person.friendly.booleanValue().and(person.cute.booleanValue()))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND(T0.friendly AND T0.cute)", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).or(person.friendly.booleanValue().and(person.cute.booleanValue()))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly AND T0.cute", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).eq(person.smart.booleanValue().and(person.cute.booleanValue()))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) =(T0.smart AND T0.cute)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).ne(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).gt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).ge(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).lt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) <(T0.smart)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).le(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) <=(T0.smart)", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).eq(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).ne(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).gt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).ge(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).lt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.cute.booleanValue()).le(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) <= ?", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().and(person.cute.booleanValue()).in(person2.alive.booleanValue().and(person2.cute.booleanValue()).asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart AND T1.cute) IN(SELECT T2.alive AND T2.cute FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().and(person.cute.booleanValue()).notIn(person2.alive.booleanValue().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart AND T1.cute) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().and(person.cute.booleanValue()).in(person.alive.booleanValue().and(person.cute.booleanValue()).asValue(), person.friendly.booleanValue().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart AND T0.cute) IN(T0.alive AND T0.cute, T0.friendly)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().and(person.cute.booleanValue()).notIn(person.alive.booleanValue().asValue(), person.friendly.booleanValue().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart AND T0.cute) NOT IN(T0.alive, T0.friendly)", sql);
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
