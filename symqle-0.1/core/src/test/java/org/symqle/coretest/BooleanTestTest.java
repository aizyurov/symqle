package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;


/**
 * @author lvovich
 */
public class BooleanTestTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().and(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS TRUE AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().or(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS TRUE OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive IS TRUE", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT NULL", sql);
    }


    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).and(person.friendly.asPredicate().isTrue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND T0.friendly IS TRUE", sql);
    }

    public void testOrNegate() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).negate().isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT(T0.alive OR T0.smart)) IS TRUE", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).or(person.friendly.asPredicate().isTrue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly IS TRUE", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().eq(person.smart.asPredicate().isTrue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) =(T0.smart IS TRUE)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().ne(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().gt(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().ge(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().lt(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) <(T0.smart)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().le(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) <=(T0.smart)", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().eq(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().ne(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().gt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().ge(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().lt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().le(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) <= ?", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.asPredicate().isTrue().in(person2.alive.asPredicate().isTrue().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart IS TRUE) IN(SELECT T2.alive IS TRUE FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.asPredicate().isTrue().notIn(person2.alive.asPredicate().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart IS TRUE) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.asPredicate().isTrue().in(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart IS TRUE) IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.asPredicate().isTrue().notIn(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart IS TRUE) NOT IN(?, ?)", sql);
   }

    public void testThen() throws Exception {
        final String sql = person.alive.asPredicate().isTrue().then(person.id).show();
        assertSimilar("SELECT CASE WHEN T0.alive IS TRUE THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.id.ge(0L).then(person.id).orWhen(person.alive.asPredicate().isTrue().thenNull()).show();
        assertSimilar("SELECT CASE WHEN T0.id >= ? THEN T0.id WHEN T0.alive IS TRUE THEN NULL END AS C0 FROM person AS T0", sql);
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
