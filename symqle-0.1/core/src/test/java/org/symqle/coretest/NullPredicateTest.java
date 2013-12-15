package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.sql.AbstractNullPredicate;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;


/**
 * @author lvovich
 */
public class NullPredicateTest extends SqlTestCase {

    public void testAsValue() throws Exception {
        final String sql = createNullPredicate().asValue().show(new GenericDialect());
        assertSimilar("SELECT T0.alive IS NULL AS C0 FROM person AS T0", sql);
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(createNullPredicate().and(person.smart.asPredicate())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL AND T0.smart", sql);
    }

    private AbstractNullPredicate createNullPredicate() {
        return person.alive.isNull();
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(createNullPredicate().or(person.smart.asPredicate())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(createNullPredicate().negate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive IS NULL", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(createNullPredicate().isTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(createNullPredicate().isNotTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(createNullPredicate().isFalse()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(createNullPredicate().isNotFalse()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(createNullPredicate().isUnknown()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(createNullPredicate().isNotUnknown()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS NOT UNKNOWN", sql);
    }

    public void testAndTwoPredicates() throws Exception {
        final String sql = person.id.where(person.alive.eq(person.smart).and(person.friendly.isNull())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive = T0.smart AND T0.friendly IS NULL", sql);
    }

    public void testOrPredicate() throws Exception {
        final String sql = person.id.where(person.alive.eq(person.smart).or(person.friendly.asPredicate())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive = T0.smart OR T0.friendly", sql);
    }

    public void testThen() throws Exception {
        final String sql = person.smart.isNull().then(person.id).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.smart IS NULL THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.smart.isNull().then(person.id).orWhen(person.alive.isNotNull().thenNull()).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.smart IS NULL THEN T0.id WHEN T0.alive IS NOT NULL THEN NULL END AS C0 FROM person AS T0", sql);
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
