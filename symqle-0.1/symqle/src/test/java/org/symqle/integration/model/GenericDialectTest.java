package org.symqle.integration.model;

import junit.framework.TestCase;
import org.symqle.sql.DebugDialect;
import org.symqle.testset.GenericDialectTestSet;

/**
 * @author lvovich
 */
public class GenericDialectTest extends TestCase implements GenericDialectTestSet {

    @Override
    public void test_showQuery_SelectStatement_Dialect_Option_1() throws Exception {
        final Employee employee = new Employee();
        final String sql = employee.firstName.showQuery(new DebugDialect());
        assertEquals("SELECT EMPLOYEE0.first_name AS C0 FROM employee AS EMPLOYEE0", sql);
    }

    @Override
    public void test_showUpdate_DataChangeStatement_Dialect_Option_1() throws Exception {
        final Employee employee = new Employee();
        final String sql = employee.delete().showUpdate(new DebugDialect());
        assertEquals("DELETE FROM employee", sql);
    }
}
