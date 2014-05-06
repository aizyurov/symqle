package org.symqle.generic;

import junit.framework.TestCase;
import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Functions;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class FunctionsTest extends TestCase {

    public void testAbs() throws Exception {
        final String sql = Functions.abs(person.id).showQuery(new GenericDialect());
        assertEquals("SELECT ABS(PERSON0.id) AS C0 FROM person AS PERSON0", sql);
    }

    public void testLn() throws Exception {
        final String sql = Functions.ln(person.id).showQuery(new GenericDialect());
        assertEquals("SELECT LN(PERSON0.id) AS C0 FROM person AS PERSON0", sql);
    }

    public void testExp() throws Exception {
        final String sql = Functions.exp(person.id).showQuery(new GenericDialect());
        assertEquals("SELECT EXP(PERSON0.id) AS C0 FROM person AS PERSON0", sql);
    }

    public void testCeil() throws Exception {
        final String sql = Functions.ceil(person.id).showQuery(new GenericDialect());
        assertEquals("SELECT CEIL(PERSON0.id) AS C0 FROM person AS PERSON0", sql);
    }

    public void testFloor() throws Exception {
        final String sql = Functions.floor(person.id).showQuery(new GenericDialect());
        assertEquals("SELECT FLOOR(PERSON0.id) AS C0 FROM person AS PERSON0", sql);
    }

    public void testSqrt() throws Exception {
        final String sql = Functions.sqrt(person.id).showQuery(new GenericDialect());
        assertEquals("SELECT SQRT(PERSON0.id) AS C0 FROM person AS PERSON0", sql);
    }

    public void testMod() throws Exception {
        final String sql = Functions.mod(person.id, DynamicParameter.create(CoreMappers.INTEGER, 2)).showQuery(new GenericDialect());
        assertEquals("SELECT MOD(PERSON0.id, ?) AS C0 FROM person AS PERSON0", sql);
    }

    public void testPower() throws Exception {
        final String sql = Functions.power(person.id, DynamicParameter.create(CoreMappers.INTEGER, 2)).showQuery(new GenericDialect());
        assertEquals("SELECT POWER(PERSON0.id, ?) AS C0 FROM person AS PERSON0", sql);
    }




    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }

        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

    private static Person person = new Person();

}
