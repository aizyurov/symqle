package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class LikePredicateTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').and(person.name.like("J%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? AND T0.name LIKE ?", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').or(person.name.like("J%"))).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? OR T0.name LIKE ?", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').negate()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.name LIKE T0.nick ESCAPE ?", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isNotTrue()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isFalse()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isNotFalse()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isUnknown()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.name.like(person.nick).escape('\\').isNotUnknown()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.name LIKE T0.nick ESCAPE ? IS NOT UNKNOWN", sql);
    }

    public void testAsValue() throws Exception {
        final String sql = person.name.like(person.nick).escape('\\').asValue().show(new GenericDialect());
        assertSimilar("SELECT T0.name LIKE T0.nick ESCAPE ? AS C0 FROM person AS T0", sql);
    }

    public void testThen() throws Exception {
        final String sql = person.name.like(person.nick).escape('\\').then(person.id).show(new GenericDialect());
        assertSimilar("SELECT CASE WHEN T0.name LIKE T0.nick ESCAPE ? THEN T0.id END AS C0 FROM person AS T0", sql);
    }

    public void testThenNull() throws Exception {
        final String sql = person.name.isNull().then(person.id).orWhen(person.name.like(person.nick).escape('\\').thenNull()).show(new GenericDialect());
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
