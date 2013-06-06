package org.simqle.mysql;

import junit.framework.TestCase;
import org.simqle.front.Params;
import org.simqle.integration.model.Employee;

/**
 * @author lvovich
 */
public class MysqlDialectTest extends TestCase {

    public void testForReadOnly() {
        final Employee employee = new Employee();
        final String sql = employee.lastName.forReadOnly().show(MysqlDialect.get());
        assertEquals("SELECT T1.last_name AS C1 FROM employee AS T1", sql);
    }

    public void testFromClauseFromNothing() {
        final String sql = Params.p(1).show(MysqlDialect.get());
        assertEquals("SELECT ? AS C1", sql);
    }

}
