package org.simqle.sql;

import org.simqle.Callback;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class NullPredicateTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.isNull().and(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.isNull().or(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.isNull().negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive IS NULL", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NULL IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.isNull().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) IS NOT NULL", sql);
    }


    public void testAndTwoPredicates() throws Exception {
        final String sql = person.id.where(person.alive.eq(person.smart).and(person.friendly.isNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive = T0.smart AND T0.friendly IS NULL", sql);
    }

    public void testOrPredicate() throws Exception {
        final String sql = person.id.where(person.alive.eq(person.smart).or(person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive = T0.smart OR T0.friendly", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.isNull().eq(person.smart.isNull())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) =(T0.smart IS NULL)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.isNull().ne(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.isNull().gt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.isNull().ge(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.isNull().lt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) <(T0.smart)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.isNull().le(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) <=(T0.smart)", sql);
    }

    public void testEqValue() throws Exception {
        final String sql = person.id.where(person.alive.isNull().eq(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) = ?", sql);
    }

    public void testNeValue() throws Exception {
        final String sql = person.id.where(person.alive.isNull().ne(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) <> ?", sql);
    }

    public void testGtValue() throws Exception {
        final String sql = person.id.where(person.alive.isNull().gt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) > ?", sql);
    }

    public void testGeValue() throws Exception {
        final String sql = person.id.where(person.alive.isNull().ge(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) >= ?", sql);
    }

    public void testLtValue() throws Exception {
        final String sql = person.id.where(person.alive.isNull().lt(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) < ?", sql);
    }

    public void testLeValue() throws Exception {
        final String sql = person.id.where(person.alive.isNull().le(true)).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive IS NULL) <= ?", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.isNull().in(person2.alive.isNull().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart IS NULL) IN(SELECT T2.alive IS NULL FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.isNull().notIn(person2.alive.booleanValue().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart IS NULL) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.isNull().in(person.alive.isNull().asValue(), person.friendly.booleanValue().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart IS NULL) IN(T0.alive IS NULL, T0.friendly)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.isNull().notIn(person.alive.booleanValue().asValue(), person.friendly.booleanValue().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart IS NULL) NOT IN(T0.alive, T0.friendly)", sql);
   }


    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<Long> alive = new LongColumn("alive", this);
        public Column<Long> smart = new LongColumn("smart", this);
        public Column<Long> cute = new LongColumn("cute", this);
        public Column<Long> speaksJapan = new LongColumn("speaks_japan", this);
        public Column<Long> friendly = new LongColumn("friendly", this);
    }

    private static Person person = new Person();
    private static Person person2 = new Person();


}
