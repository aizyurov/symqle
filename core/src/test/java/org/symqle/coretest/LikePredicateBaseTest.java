package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.sql.AbstractLikePredicateBase;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class LikePredicateBaseTest extends SqlTestCase {

    private AbstractLikePredicateBase createLikePredicateBase() {
        return person.name.like(person.nick);
    }

    public void testAsCondition() throws Exception {
        final String sql = person.id.where(createLikePredicateBase()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick", sql);
    }

    public void testAdapt() throws Exception {
        // only a trivial case is possible
        final String sql1 = person.id.where(createLikePredicateBase()).showQuery(new GenericDialect());
        final String sql2 = person.id.where(AbstractLikePredicateBase.adapt(createLikePredicateBase())).showQuery(new GenericDialect());
        assertEquals(sql1, sql2);
    }

    public void testEscape() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().escape(DynamicParameter.create(CoreMappers.STRING, "\\"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ?", sql);
    }

    public void testEscapeChar() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().escape('\\')).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ?", sql);
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().and(person.name.like("J%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick AND T0.name LIKE ?", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().or(person.name.like("J%"))).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick OR T0.name LIKE ?", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().negate()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.name LIKE T0.nick", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().isTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().isNotTrue()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().isFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().isNotFalse()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().isUnknown()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(createLikePredicateBase().isNotUnknown()).showQuery(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS NOT UNKNOWN", sql);
    }

    public void testAsValue() throws Exception {
        final String sql = createLikePredicateBase().asValue().showQuery(new GenericDialect());
        assertSimilar("SELECT T0.name LIKE T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testThen() throws Exception {
        final String sql = createLikePredicateBase().then(person.id).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name LIKE T0.nick THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.name.isNull().then(person.id).orWhen(createLikePredicateBase().thenNull()).showQuery(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name IS NULL THEN T0.id WHEN T0.name LIKE T0.nick THEN NULL END AS C0 FROM person AS T0", sql);
    }



    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<String> nick = defineColumn(CoreMappers.STRING, "nick");
        public Column<Boolean> alive = defineColumn(CoreMappers.BOOLEAN, "alive");
        public Column<Long> parentId = defineColumn(CoreMappers.LONG, "parent_id");
    }

    private static Person person = new Person();

    private static Person child = new Person();



}
