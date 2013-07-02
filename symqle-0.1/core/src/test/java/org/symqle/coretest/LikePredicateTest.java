package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class LikePredicateTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').and(person.name.like("J%"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? AND T0.name LIKE ?", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').or(person.name.like("J%"))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? OR T0.name LIKE ?", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.name LIKE T0.nick ESCAPE ?", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) IS NOT NULL", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').eq(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) = T0.alive", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').ne(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) <> T0.alive", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').gt(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) > T0.alive", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').ge(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) >= T0.alive", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').lt(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) < T0.alive", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').le(person.alive)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) <= T0.alive", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').eq(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').ne(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').gt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').ge(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').lt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').le(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) <= ?", sql);
    }

    public void testIn() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').in(child.alive.where(child.parentId.eq(person.id)))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) IN(SELECT T1.alive FROM person AS T1 WHERE T1.parent_id = T0.id)", sql);
    }

    public void testNotIn() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').notIn(child.alive.where(child.parentId.eq(person.id)))).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) NOT IN(SELECT T1.alive FROM person AS T1 WHERE T1.parent_id = T0.id)", sql);
    }

    public void testInList() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').in(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) IN(?, ?)", sql);
   }

    public void testNotInList() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').notIn(true, false)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.name LIKE T0.nick ESCAPE ?) NOT IN(?, ?)", sql);
   }

    public void testAsValue() throws Exception {
        final String sql = person.name.like(person.nick).escape('\\').asValue().show();
        assertSimilar("SELECT T0.name LIKE T0.nick ESCAPE ? AS C0 FROM person AS T0", sql);
    }

    public void testThen() throws Exception {
        final String sql = person.name.like(person.nick).escape('\\').then(person.id).show();
        assertSimilar("SELECT CASE WHEN T0.name LIKE T0.nick ESCAPE ? THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.name.isNull().then(person.id).orWhen(person.name.like(person.nick).escape('\\').thenNull()).show();
        assertSimilar("SELECT CASE WHEN T0.name IS NULL THEN T0.id WHEN T0.name LIKE T0.nick ESCAPE ? THEN NULL END AS C0 FROM person AS T0", sql);
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
