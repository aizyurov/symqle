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
public class BooleanPrimaryTest extends SqlTestCase {

    public void testAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive AND T0.smart", sql);
    }

    public void testOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart", sql);
    }

    public void testNegate() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT T0.alive", sql);
    }

    public void testIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS TRUE", sql);
    }

    public void testIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NOT TRUE", sql);
    }

    public void testIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS FALSE", sql);
    }

    public void testIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NOT FALSE", sql);
    }

    public void testIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS UNKNOWN", sql);
    }

    public void testIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive IS NOT UNKNOWN", sql);
    }

    public void testIsNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive) IS NULL", sql);
    }

    public void testIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive) IS NOT NULL", sql);
    }


    public void testOrAnd() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).and(person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) AND T0.friendly", sql);
    }

    public void testOrNegate() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).negate()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE NOT(T0.alive OR T0.smart)", sql);
    }

    public void testOrOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).or(person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive OR T0.smart OR T0.friendly", sql);
    }

    public void testOrIsTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).isTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS TRUE", sql);
    }

    public void testOrIsNotTrue() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).isNotTrue()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS NOT TRUE", sql);
    }

    public void testOrIsFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).isFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS FALSE", sql);
    }

    public void testOrIsNotFalse() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).isNotFalse()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS NOT FALSE", sql);
    }

    public void testOrIsUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).isUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS UNKNOWN", sql);
    }

    public void testOrIsNotUnknown() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).isNotUnknown()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS NOT UNKNOWN", sql);
    }

    public void testOrIsNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).isNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS NULL", sql);
    }

    public void testOrIsNotNull() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().or(person.smart.booleanValue()).isNotNull()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive OR T0.smart) IS NOT NULL", sql);
    }

    public void testAndOr() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().and(person.smart.booleanValue()).or(person.friendly.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.alive AND T0.smart OR T0.friendly", sql);
    }

    public void testEq() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().eq(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive) =(T0.smart)", sql);
    }

    public void testNe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().ne(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive) <>(T0.smart)", sql);
    }

    public void testGt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().gt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive) >(T0.smart)", sql);
    }

    public void testGe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().ge(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive) >=(T0.smart)", sql);
    }

    public void testLt() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().lt(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive) <(T0.smart)", sql);
    }

    public void testLe() throws Exception {
        final String sql = person.id.where(person.alive.booleanValue().le(person.smart.booleanValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.alive) <=(T0.smart)", sql);
    }

    public void testIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().in(person2.alive.booleanValue().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart) IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testNotIn() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().notIn(person2.alive.booleanValue().asValue())).show();
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE(T1.smart) NOT IN(SELECT T2.alive FROM person AS T2)", sql);
    }

    public void testInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().in(person.alive.booleanValue().asValue(), person.friendly.booleanValue().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart) IN(T0.alive, T0.friendly)", sql);
   }

    public void testNotInList() throws Exception {
        String sql = person.id.where(person.smart.booleanValue().notIn(person.alive.booleanValue().asValue(), person.friendly.booleanValue().asValue())).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 WHERE(T0.smart) NOT IN(T0.alive, T0.friendly)", sql);
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
