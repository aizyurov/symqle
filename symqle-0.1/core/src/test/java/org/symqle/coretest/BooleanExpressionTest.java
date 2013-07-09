package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.sql.AbstractQuerySpecification;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class BooleanExpressionTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).and(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).or(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.cute OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(T0.alive OR T0.cute)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final AbstractQuerySpecification<Long> where = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).isUnknown());
        final String sql = where.orderBy(person.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS UNKNOWN ORDER BY T0.id", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT NULL", sql);
    }


    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).and(person.friendly.asPredicate().or(person.cute.asPredicate()))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND(T0.friendly OR T0.cute)", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).or(person.friendly.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).eq(person.smart.asPredicate().or(person.cute.asPredicate()))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) =(T0.smart OR T0.cute)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).ne(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).gt(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).ge(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).lt(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) <(T0.smart)", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).eq(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).ne(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) <> ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).lt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).le(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) <= ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).gt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).ge(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) >= ?", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.asPredicate().or(person.cute.asPredicate()).in(person2.alive.asPredicate().or(person2.cute.asPredicate()).asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart OR T1.cute) IN(SELECT T2.alive OR T2.cute FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.asPredicate().or(person.cute.asPredicate()).notIn(person2.alive.asPredicate().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart OR T1.cute) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.asPredicate().or(person.cute.asPredicate()).in(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart OR T0.cute) IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.asPredicate().or(person.cute.asPredicate()).notIn(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart OR T0.cute) NOT IN(?, ?)", sql);
   }


    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.cute.asPredicate()).le(person.smart.asPredicate())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) <=(T0.smart)", sql);
    }

    public void testThen() throws Exception {
        final String sql = person.alive.asPredicate().or(person.cute.asPredicate()).then(person.id).show();
        assertSimilar("SELECT CASE WHEN T0.alive OR T0.cute THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.id.ge(0L).then(person.id).orWhen(person.alive.asPredicate().or(person.cute.asPredicate()).thenNull()).show();
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
