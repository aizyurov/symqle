package org.simqle.mysql;

import junit.framework.TestCase;
import org.simqle.front.Params;
import org.simqle.integration.model.Employee;
import org.simqle.jdbc.Option;

/**
 * @author lvovich
 */
public class MysqlDialectTest extends TestCase {

    public void testForReadOnly() {
        final Employee employee = new Employee();
        final String sql = employee.lastName.forReadOnly().show(MySqlDialect.get());
        assertEquals("SELECT T1.last_name AS C1 FROM employee AS T1", sql);
    }

    public void testFromClauseFromNothing() {
        final String sql = Params.p(1).show(MySqlDialect.get(), Option.allowNoTables(true));
        assertEquals("SELECT ? AS C1", sql);
    }

    public void testValueExpression_is_BooleanExpression() {
        final Employee employee = new Employee();
        final String sql = employee.lastName.where(employee.deptId.isNotNull().asValue().like("1%")).show(MySqlDialect.get());
        assertEquals("SELECT T1.last_name AS C1 FROM employee AS T1 WHERE(T1.dept_id IS NOT NULL) LIKE ?", sql);
    }

    // demonstrate double parenthesizing (one for BooleanExpression->ValueExpression, the other for ValueExpression->ValueExpressionPrimary
    // final step is ValueExpressionPrimary->StringExpression, it does not add extra ()
    // this is ugly, but works. The use case is very artificial
    public void testValueExpression_is_BooleanExpression2() {
        final Employee employee = new Employee();
        final String sql = employee.lastName.where(employee.deptId.isNotNull().asValue().eq(true)).show(MySqlDialect.get());
        assertEquals("SELECT T1.last_name AS C1 FROM employee AS T1 WHERE((T1.dept_id IS NOT NULL)) = ?", sql);
    }

}
