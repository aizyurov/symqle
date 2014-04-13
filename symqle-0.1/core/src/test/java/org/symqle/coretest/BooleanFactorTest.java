package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.sql.AbstractBooleanFactor;
import org.symqle.sql.AbstractBooleanPrimary;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;


/**
 * @author lvovich
 */
public class BooleanFactorTest extends SqlTestCase {

    private AbstractBooleanFactor createBooleanFactor() {
        return person.alive.asBoolean().negate();
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(createBooleanFactor().and(person.smart.asBoolean())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive AND T0.smart", sql);
    }

    public void testAdapt() throws Exception {
        final AbstractBooleanPrimary abstractBooleanPrimary = person.alive.asBoolean();
        final AbstractBooleanFactor adaptor = AbstractBooleanFactor.adapt(abstractBooleanPrimary);
        final String sql1 = person.id.where(abstractBooleanPrimary).showQuery(new GenericDialect());
        final String sql2 = person.id.where(adaptor).showQuery(new GenericDialect());
        assertEquals(sql1, sql2);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(createBooleanFactor().or(person.smart.asBoolean())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(createBooleanFactor().negate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(NOT T0.alive)", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(createBooleanFactor().isNotTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(createBooleanFactor().isFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(createBooleanFactor().isNotFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(createBooleanFactor().isUnknown()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(createBooleanFactor().isNotUnknown()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(NOT T0.alive) IS NOT UNKNOWN", sql);
    }

    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.asBoolean().or(person.smart.asBoolean()).and(person.friendly.asBoolean().negate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND NOT T0.friendly", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.asBoolean().or(person.smart.asBoolean()).or(person.friendly.asBoolean().negate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR NOT T0.friendly", sql);
    }

    public void testThen() throws Exception {
        final String sql = createBooleanFactor().then(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN NOT T0.alive THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.id.ge(0L).then(person.id).orWhen(createBooleanFactor().thenNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.id >= ? THEN T0.id WHEN NOT T0.alive THEN NULL END AS C0 FROM person AS T0", sql);
    }

    public void testAsValue() throws Exception {
        final String sql = createBooleanFactor().asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT NOT T0.alive AS C0 FROM person AS T0", sql);
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
