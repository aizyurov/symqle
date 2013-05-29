package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.TableOrView;


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
        String sql = person.id.where(person.smart.booleanValue().negate().in(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.smart) IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().negate().notIn(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.smart) NOT IN(?, ?)", sql);
   }


    public void testThen() throws Exception {
        final String sql = person.alive.booleanValue().negate().then(person.id).show();
        assertSimilar("SELECT CASE WHEN NOT T0.alive THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.id.ge(0L).then(person.id).orWhen(person.alive.booleanValue().negate().thenNull()).show();
        assertSimilar("SELECT CASE WHEN T0.id >= ? THEN T0.id WHEN NOT T0.alive THEN NULL END AS C0 FROM person AS T0", sql);
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
