package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.sql.*;
import org.symqle.testset.AbstractBooleanExpressionTestSet;

/**
 * @author lvovich
 */
public class BooleanExpressionTest extends SqlTestCase implements AbstractBooleanExpressionTestSet {

    private AbstractBooleanExpression createBooleanExpression() {
        return person.alive.asPredicate().or(person.cute.asPredicate());
    }

    @Override
    public void test_adapt_BooleanExpression() {
        final AbstractBooleanPrimary abstractBooleanPrimary = person.alive.asPredicate();
        final AbstractBooleanExpression abstractBooleanExpression = AbstractBooleanExpression.adapt(abstractBooleanPrimary);
        final String sql1 = person.id.where(abstractBooleanPrimary).showQuery(new GenericDialect());
        final String sql2 = person.id.where(abstractBooleanExpression).showQuery(new GenericDialect());
        assertEquals(sql1, sql2);
    }

    @Override
    public void test_and_BooleanFactor() {
        final String sql = person.id.where(createBooleanExpression().and(person.smart.asPredicate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) AND T0.smart", sql);
    }

    @Override
    public void test_and_BooleanTerm_BooleanFactor_1() {
        final BooleanTerm abstractBooleanPrimary = person.smart.asPredicate();
        final String sql = person.id.where(abstractBooleanPrimary.and(createBooleanExpression())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.smart AND(T0.alive OR T0.cute)", sql);
    }

    @Override
    public void test_asValue_() {
        final String sql = createBooleanExpression().asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.alive OR T0.cute AS C0 FROM person AS T0", sql);
    }

    @Override
    public void test_isFalse_() {
        final String sql = person.id.where(createBooleanExpression().isFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS FALSE", sql);
    }

    @Override
    public void test_isNotFalse_() {
        final String sql = person.id.where(createBooleanExpression().isNotFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT FALSE", sql);
    }

    @Override
    public void test_isNotTrue_() {
        final String sql = person.id.where(createBooleanExpression().isNotTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT TRUE", sql);
    }

    @Override
    public void test_isNotUnknown_() {
        final String sql = person.id.where(createBooleanExpression().isNotUnknown()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS NOT UNKNOWN", sql);
    }

    @Override
    public void test_isTrue_() {
        final String sql = person.id.where(createBooleanExpression().isTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS TRUE", sql);
    }

    @Override
    public void test_isUnknown_() {
        final AbstractQuerySpecificationScalar<Long> querySpecificationScalar = person.id.where(createBooleanExpression().isUnknown());
        final String sql = querySpecificationScalar.orderBy(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.cute) IS UNKNOWN ORDER BY T0.id", sql);
    }

    @Override
    public void test_negate_() {
        final String sql = person.id.where(createBooleanExpression().negate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(T0.alive OR T0.cute)", sql);
    }

    @Override
    public void test_or_BooleanExpression_BooleanTerm_1() {
        final String sql = person.id.where(person.alive.asPredicate().or(person.smart.asPredicate()).or(createBooleanExpression())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR(T0.alive OR T0.cute)", sql);
    }

    @Override
    public void test_or_BooleanTerm() {
        final String sql = person.id.where(createBooleanExpression().or(person.smart.asPredicate())).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.cute OR T0.smart", sql);
    }

    @Override
    public void test_thenNull_() {
        final String sql = person.id.ge(0L).then(person.id).orWhen(createBooleanExpression().thenNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.id >= ? THEN T0.id WHEN T0.alive OR T0.cute THEN NULL END AS C0 FROM person AS T0", sql);
    }

    @Override
    public void test_then_ValueExpression() {
        final String sql = createBooleanExpression().then(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.alive OR T0.cute THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    @Override
    public void test_where_DeleteStatementBase_WhereClause_1() {
        final String sql = person.delete().where(createBooleanExpression()).showUpdate(new GenericDialect());
        assertSimilar("DELETE FROM person WHERE person.alive OR person.cute", sql);
    }

    @Override
    public void test_where_QueryBaseScalar_WhereClause_1() {
        final String sql = person.id.where(createBooleanExpression()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.cute", sql);

    }

    @Override
    public void test_where_QueryBase_WhereClause_1() {
        final String sql = person.id.pair(person.alive).where(createBooleanExpression()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.alive AS C1 FROM person AS T0 WHERE T0.alive OR T0.cute", sql);
    }

    @Override
    public void test_where_AggregateQueryBase_WhereClause_1() throws Exception {
        final String sql = person.id.count().where(createBooleanExpression()).showQuery(new GenericDialect());
        assertSimilar("SELECT COUNT(T0.id) AS C0 FROM person AS T0 WHERE T0.alive OR T0.cute", sql);
    }

    @Override
    public void test_where_UpdateStatementBase_WhereClause_1() {
        final String sql = person.update(person.alive.set(true)).where(createBooleanExpression()).showUpdate(new GenericDialect());
        assertSimilar("UPDATE person SET alive = ? WHERE person.alive OR person.cute", sql);
    }

    private static class Person extends Table {
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
