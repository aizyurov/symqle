package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.sql.AbstractBooleanPrimary;
import org.symqle.sql.AbstractBooleanTerm;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;


/**
 * @author lvovich
 */
public class BooleanTermTest extends SqlTestCase {

    private AbstractBooleanTerm createBooleanTerm() {
        return person.alive.asPredicate().and(person.cute.asPredicate());
    }

    public void testAdapt() throws Exception {
        final AbstractBooleanPrimary abstractBooleanPrimary = person.alive.asPredicate();
        final AbstractBooleanTerm adaptor = AbstractBooleanTerm.adapt(abstractBooleanPrimary);
        final String sql1 = person.id.where(abstractBooleanPrimary).show(new GenericDialect());
        final String sql2 = person.id.where(adaptor).show(new GenericDialect());
        assertEquals(sql1, sql2);

    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(createBooleanTerm().and(person.smart.asPredicate())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive AND T0.cute AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(createBooleanTerm().or(person.smart.asPredicate())).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive AND T0.cute OR T0.smart", sql);
    }


    public void testNegate() throws Exception {
        final String sql = person.id.where(createBooleanTerm().negate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(T0.alive AND T0.cute)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(createBooleanTerm().isTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(createBooleanTerm().isNotTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(createBooleanTerm().isFalse()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(createBooleanTerm().isNotFalse()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(createBooleanTerm().isUnknown()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(createBooleanTerm().isNotUnknown()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive AND T0.cute) IS NOT UNKNOWN", sql);
    }


    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).and(person.friendly.asPredicate().and(person.cute.asPredicate()))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND(T0.friendly AND T0.cute)", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).or(person.friendly.asPredicate().and(person.cute.asPredicate()))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly AND T0.cute", sql);
    }

    public void testThen() throws Exception {
        final String sql = createBooleanTerm().then(person.id).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.alive AND T0.cute THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.id.ge(0L).then(person.id).orWhen(createBooleanTerm().thenNull()).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.id >= ? THEN T0.id WHEN T0.alive AND T0.cute THEN NULL END AS C0 FROM person AS T0", sql);
    }

    public void testAsValue() throws Exception {
        final String sql = createBooleanTerm().asValue().show(new GenericDialect());
        assertSimilar("SELECT T0.alive AND T0.cute AS C0 FROM person AS T0", sql);
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
