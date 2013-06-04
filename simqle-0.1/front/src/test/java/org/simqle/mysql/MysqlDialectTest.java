package org.simqle.mysql;

import junit.framework.TestCase;
import org.simqle.Mappers;
import org.simqle.front.Params;
import org.simqle.sql.Column;
import org.simqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class MysqlDialectTest extends TestCase {

    public void testForReadOnly() {
        final Employee employee = new Employee();
        final String sql = employee.name.forReadOnly().show(MysqlDialect.get());
        assertEquals("SELECT T1.name AS C1 FROM employee AS T1", sql);
    }

    public void testFromClauseFromNothing() {
        final String sql = Params.p(1).show(MysqlDialect.get());
        assertEquals("SELECT ? AS C1", sql);
    }

    public void testConcat() {
        final Employee employee = new Employee();
        try {
            final String sql = employee.name.concat("#").show(MysqlDialect.get());
            fail("concat should not be supported");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().startsWith("concat operator is not supported by this dialect"));
        }

    }



    private class Employee extends TableOrView {
        private Employee() {
            super("employee");
        }

        public final Column<String> name = defineColumn(Mappers.STRING, "name");
    }
}
