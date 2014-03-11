package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.sql.AbstractBooleanExpression;
import org.symqle.sql.AbstractBooleanFactor;
import org.symqle.sql.AbstractBooleanPrimary;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;


/**
 * @author lvovich
 */
public class BooleanPrimaryTest extends SqlTestCase {

    private AbstractBooleanPrimary createBooleanPrimary() {
        return person.alive.asPredicate();
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().and(person.smart.asPredicate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive AND T0.smart", sql);
    }

    public void testAdapt() throws Exception {
        final AbstractBooleanExpression booleanExpression = person.alive.asPredicate().or(person.smart.asPredicate());
        final AbstractBooleanPrimary adaptor = AbstractBooleanPrimary.adapt(booleanExpression);
        final AbstractBooleanFactor factor1 = booleanExpression.negate();
        final AbstractBooleanFactor factor2 = adaptor.negate();
        final String sql1 = person.id.where(factor1).showQuery(new GenericDialect());
        final String sql2 = person.id.where(factor2).showQuery(new GenericDialect());
        assertEquals(sql1, sql2);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().or(person.smart.asPredicate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().negate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().isTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().isNotTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().isFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().isNotFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().isUnknown()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().isNotUnknown()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NOT UNKNOWN", sql);
    }

    public void testOrAnd() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().or(person.smart.asPredicate()).and(person.friendly.asPredicate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND T0.friendly", sql);
    }

    public void testOrNegate() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().or(person.smart.asPredicate()).negate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(T0.alive OR T0.smart)", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().or(person.smart.asPredicate()).or(person.friendly.asPredicate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly", sql);
    }

    public void testOrIsTrue() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().or(person.smart.asPredicate()).isTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS TRUE", sql);
    }

    public void testOrIsNotTrue() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().or(person.smart.asPredicate()).isNotTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS NOT TRUE", sql);
    }

    public void testOrIsFalse() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().or(person.smart.asPredicate()).isFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS FALSE", sql);
    }

    public void testOrIsNotFalse() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().or(person.smart.asPredicate()).isNotFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS NOT FALSE", sql);
    }

    public void testOrIsUnknown() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().or(person.smart.asPredicate()).isUnknown()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS UNKNOWN", sql);
    }

    public void testOrIsNotUnknown() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().or(person.smart.asPredicate()).isNotUnknown()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS NOT UNKNOWN", sql);
    }

    public void testAndOr() throws Exception {
        final String sql = person.id.where(createBooleanPrimary().and(person.smart.asPredicate()).or(person.friendly.asPredicate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive AND T0.smart OR T0.friendly", sql);
    }

    public void testThen() throws Exception {
        final String sql = createBooleanPrimary().then(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.alive THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.id.ge(0L).then(person.id).orWhen(createBooleanPrimary().thenNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.id >= ? THEN T0.id WHEN T0.alive THEN NULL END AS C0 FROM person AS T0", sql);
    }

    public void testAsValue() throws Exception {
        final String sql = createBooleanPrimary().asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.alive AS C0 FROM person AS T0", sql);
    }

    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }

        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<Boolean> alive = defineColumn(CoreMappers.BOOLEAN, "alive");
        public Column<Boolean> smart = defineColumn(CoreMappers.BOOLEAN, "smart");
        public Column<Boolean> cute = defineColumn(CoreMappers.BOOLEAN, "cute");
        public Column<Boolean> friendly = defineColumn(CoreMappers.BOOLEAN, "friendly");
    }

    private static Person person = new Person();
    private static Person person2 = new Person();


}
