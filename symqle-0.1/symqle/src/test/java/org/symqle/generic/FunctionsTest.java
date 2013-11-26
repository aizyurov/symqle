package org.symqle.generic;

import junit.framework.TestCase;
import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class FunctionsTest extends TestCase {

    public void testAbs() throws Exception {
        final String sql = Functions.abs(person.id).show(new GenericDialect());
        assertEquals("SELECT ABS(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testLn() throws Exception {
        final String sql = Functions.ln(person.id).show(new GenericDialect());
        assertEquals("SELECT LN(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testExp() throws Exception {
        final String sql = Functions.exp(person.id).show(new GenericDialect());
        assertEquals("SELECT EXP(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testCeil() throws Exception {
        final String sql = Functions.ceil(person.id).show(new GenericDialect());
        assertEquals("SELECT CEIL(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testFloor() throws Exception {
        final String sql = Functions.floor(person.id).show(new GenericDialect());
        assertEquals("SELECT FLOOR(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testSqrt() throws Exception {
        final String sql = Functions.sqrt(person.id).show(new GenericDialect());
        assertEquals("SELECT SQRT(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testMod() throws Exception {
        final String sql = Functions.mod(person.id, DynamicParameter.create(Mappers.INTEGER, 2)).show(new GenericDialect());
        assertEquals("SELECT MOD(T0.id, ?) AS C0 FROM person AS T0", sql);
    }

    public void testPower() throws Exception {
        final String sql = Functions.power(person.id, DynamicParameter.create(Mappers.INTEGER, 2)).show(new GenericDialect());
        assertEquals("SELECT POWER(T0.id, ?) AS C0 FROM person AS T0", sql);
    }




    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();

}
