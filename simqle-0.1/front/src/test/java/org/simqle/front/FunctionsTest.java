package org.simqle.front;

import junit.framework.TestCase;
import org.simqle.Mappers;
import org.simqle.generic.Functions;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class FunctionsTest extends TestCase {

    public void testAbs() throws Exception {
        final String sql = Functions.abs(person.id).show();
        assertEquals("SELECT ABS(T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testLn() throws Exception {
        final String sql = Functions.ln(person.id).show();
        assertEquals("SELECT LN(T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testExp() throws Exception {
        final String sql = Functions.exp(person.id).show();
        assertEquals("SELECT EXP(T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testCeil() throws Exception {
        final String sql = Functions.ceil(person.id).show();
        assertEquals("SELECT CEIL(T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testFloor() throws Exception {
        final String sql = Functions.floor(person.id).show();
        assertEquals("SELECT FLOOR(T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testSqrt() throws Exception {
        final String sql = Functions.sqrt(person.id).show();
        assertEquals("SELECT SQRT(T1.id) AS C1 FROM person AS T1", sql);
    }

    public void testMod() throws Exception {
        final String sql = Functions.mod(person.id, DynamicParameter.create(Mappers.INTEGER, 2)).show();
        assertEquals("SELECT MOD(T1.id, ?) AS C1 FROM person AS T1", sql);
    }

    public void testPower() throws Exception {
        final String sql = Functions.power(person.id, DynamicParameter.create(Mappers.INTEGER, 2)).show();
        assertEquals("SELECT POWER(T1.id, ?) AS C1 FROM person AS T1", sql);
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
