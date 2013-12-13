package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;


/**
 * @author lvovich
 */
public class BooleanTestTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().and(person.smart.asPredicate())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS TRUE AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().or(person.smart.asPredicate())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS TRUE OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().negate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive IS TRUE", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isNotTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isFalse()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isNotFalse()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isUnknown()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().isTrue().isNotUnknown()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS TRUE) IS NOT UNKNOWN", sql);
    }

    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).and(person.friendly.asPredicate().isTrue())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND T0.friendly IS TRUE", sql);
    }

    public void testOrNegate() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).negate().isTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT(T0.alive OR T0.smart)) IS TRUE", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).or(person.friendly.asPredicate().isTrue())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly IS TRUE", sql);
    }

    public void testThen() throws Exception {
        final String sql = person.alive.asPredicate().isTrue().then(person.id).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.alive IS TRUE THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.id.ge(0L).then(person.id).orWhen(person.alive.asPredicate().isTrue().thenNull()).show(new GenericDialect());
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
