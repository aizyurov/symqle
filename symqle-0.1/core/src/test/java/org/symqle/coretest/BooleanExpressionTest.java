package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.sql.*;

/**
 * @author lvovich
 */
public class BooleanExpressionTest extends SqlTestCase {

    private AbstractBooleanExpression createBooleanExpression() {
        return person.alive.asPredicate().or(person.cute.asPredicate());
    }

    public void testAdapt() throws Exception {
        final AbstractBooleanPrimary abstractBooleanPrimary = person.alive.asPredicate();
        final AbstractBooleanExpression abstractBooleanExpression = AbstractBooleanExpression.adapt(abstractBooleanPrimary);
        final String sql1 = person.id.where(abstractBooleanPrimary).showQuery(new GenericDialect());
        final String sql2 = person.id.where(abstractBooleanExpression).showQuery(new GenericDialect());
        assertEquals(sql1, sql2);
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(createBooleanExpression().and(person.smart.asPredicate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(createBooleanExpression().or(person.smart.asPredicate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.cute OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(createBooleanExpression().negate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(T0.alive OR T0.cute)", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(createBooleanExpression().isTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(createBooleanExpression().isNotTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(createBooleanExpression().isFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(createBooleanExpression().isNotFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final AbstractQuerySpecificationScalar<Long> querySpecificationScalar = person.id.where(createBooleanExpression().isUnknown());
        final String sql = querySpecificationScalar.orderBy(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS UNKNOWN ORDER BY T0.id", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(createBooleanExpression().isNotUnknown()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT UNKNOWN", sql);
    }

    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).and(person.friendly.asPredicate().or(person.cute.asPredicate()))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND(T0.friendly OR T0.cute)", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).or(person.friendly.asPredicate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly", sql);
    }

    public void testThen() throws Exception {
        final String sql = createBooleanExpression().then(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.alive OR T0.cute THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.id.ge(0L).then(person.id).orWhen(createBooleanExpression().thenNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.id >= ? THEN T0.id WHEN T0.alive OR T0.cute THEN NULL END AS C0 FROM person AS T0", sql);
    }

    public void testAsValue() throws Exception {
        final String sql = createBooleanExpression().asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.alive OR T0.cute AS C0 FROM person AS T0", sql);
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
