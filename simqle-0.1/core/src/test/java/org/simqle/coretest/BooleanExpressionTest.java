package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class BooleanExpressionTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).and(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).or(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.cute OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(T0.alive OR T0.cute)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT NULL", sql);
    }


    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).and(person.friendly.booleanValue().or(person.cute.booleanValue()))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND(T0.friendly OR T0.cute)", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).or(person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).eq(person.smart.booleanValue().or(person.cute.booleanValue()))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) =(T0.smart OR T0.cute)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).ne(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).gt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).ge(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).lt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) <(T0.smart)", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).eq(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).ne(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) <> ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).lt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).le(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) <= ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).gt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).ge(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) >= ?", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().or(person.cute.booleanValue()).in(person2.alive.booleanValue().or(person2.cute.booleanValue()).asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart OR T1.cute) IN(SELECT T2.alive OR T2.cute FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().or(person.cute.booleanValue()).notIn(person2.alive.booleanValue().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart OR T1.cute) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().or(person.cute.booleanValue()).in(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart OR T0.cute) IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().or(person.cute.booleanValue()).notIn(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart OR T0.cute) NOT IN(?, ?)", sql);
   }


    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.cute.booleanValue()).le(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) <=(T0.smart)", sql);
    }

    public void testThen() throws Exception {
        final String sql = person.alive.booleanValue().or(person.cute.booleanValue()).then(person.id).show();
        assertSimilar("SELECT CASE WHEN T0.alive OR T0.cute THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.id.ge(0L).then(person.id).orWhen(person.alive.booleanValue().or(person.cute.booleanValue()).thenNull()).show();
        assertSimilar("SELECT CASE WHEN T0.id >= ? THEN T0.id WHEN T0.alive OR T0.cute THEN NULL END AS C0 FROM person AS T0", sql);
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Boolean> alive = defineColumn(Mappers.BOOLEAN, "alive");
        public Column<Boolean> smart = defineColumn(Mappers.BOOLEAN, "smart");
        public Column<Boolean> cute = defineColumn(Mappers.BOOLEAN, "cute");
        public Column<Boolean> friendly = defineColumn(Mappers.BOOLEAN, "friendly");
    }

    private static Person person = new Person();
    private static Person person2 = new Person();


}
