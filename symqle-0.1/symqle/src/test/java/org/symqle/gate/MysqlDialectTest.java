package org.symqle.gate;

import junit.framework.TestCase;
import org.symqle.generic.Params;
import org.symqle.integration.model.Employee;
import org.symqle.jdbc.Option;

/**
 * @author lvovich
 */
public class MysqlDialectTest extends TestCase {

    public void testForReadOnly() {
        final Employee employee = new Employee();
        final String sql = employee.lastName.forReadOnly().show(new MySqlDialect());
        assertEquals("SELECT T1.last_name AS C1 FROM employee AS T1", sql);
    }

    public void testFromClauseFromNothing() {
        final String sql = Params.p(1).show(new MySqlDialect(), Option.allowNoTables(true));
        assertEquals("SELECT ? AS C1", sql);
    }

    public void testValueExpression_is_BooleanExpression() {
        final Employee employee = new Employee();
        final String sql = employee.lastName.where(employee.deptId.isNotNull().asValue().like("1%")).show(new MySqlDialect());
        assertEquals("SELECT T1.last_name AS C1 FROM employee AS T1 WHERE(T1.dept_id IS NOT NULL) LIKE ?", sql);
    }

    // demonstrate double parenthesizing (one for BooleanExpression->ValueExpression, the other for ValueExpression->ValueExpressionPrimary
    // final step is ValueExpressionPrimary->StringExpression, it does not add extra ()
    // this is ugly, but works. The use case is very artificial
    public void testValueExpression_is_BooleanExpression2() {
        final Employee employee = new Employee();
        final String sql = employee.lastName.where(employee.deptId.isNotNull().asValue().eq(true)).show(new MySqlDialect());
        assertEquals("SELECT T1.last_name AS C1 FROM employee AS T1 WHERE((T1.dept_id IS NOT NULL)) = ?", sql);
    }

}
