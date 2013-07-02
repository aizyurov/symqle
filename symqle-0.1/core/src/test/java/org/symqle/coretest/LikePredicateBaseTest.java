package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class LikePredicateBaseTest extends SqlTestCase {

    public void testAsCondition() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick", sql);
    }

    public void testEscape() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape(DynamicParameter.create(Mappers.STRING, "\\"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ?", sql);
    }

    public void testEscapeChar() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\')).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ?", sql);
    }

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).and(person.name.like("J%"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick AND T0.name LIKE ?", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).or(person.name.like("J%"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick OR T0.name LIKE ?", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.name LIKE T0.nick", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).eq(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) = T0.alive", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).ne(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) <> T0.alive", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).gt(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) > T0.alive", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).ge(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) >= T0.alive", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).lt(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) < T0.alive", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).le(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) <= T0.alive", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).eq(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).ne(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).gt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).ge(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).lt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).le(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) <= ?", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).in(child.alive.where(child.parentId.eq(person.id)))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) IN(SELECT T1.alive FROM person AS T1 WHERE T1.parent_id = T0.id)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).notIn(child.alive.where(child.parentId.eq(person.id)))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) NOT IN(SELECT T1.alive FROM person AS T1 WHERE T1.parent_id = T0.id)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).in(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).notIn(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick) NOT IN(?, ?)", sql);
   }

    public void testAsValue() throws Exception {
        final String sql = person.name.like(person.nick).asValue().show();
        assertSimilar("SELECT T0.name LIKE T0.nick AS C0 FROM person AS T0", sql);
    }

    public void testThen() throws Exception {
        final String sql = person.name.like(person.nick).then(person.id).show();
        assertSimilar("SELECT CASE WHEN T0.name LIKE T0.nick THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.name.isNull().then(person.id).orWhen(person.name.like(person.nick).thenNull()).show();
        assertSimilar("SELECT CASE WHEN T0.name IS NULL THEN T0.id WHEN T0.name LIKE T0.nick THEN NULL END AS C0 FROM person AS T0", sql);
    }



    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<String> nick = defineColumn(Mappers.STRING, "nick");
        public Column<Boolean> alive = defineColumn(Mappers.BOOLEAN, "alive");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
    }

    private static Person person = new Person();

    private static Person child = new Person();



}
