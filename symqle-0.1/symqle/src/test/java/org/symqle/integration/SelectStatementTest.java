package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractSelectStatement;
import org.symqle.testset.AbstractSelectStatementTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class SelectStatementTest extends AbstractIntegrationTestBase implements AbstractSelectStatementTestSet {

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createStatement(employee).compileQuery(getEngine()).list();
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    private AbstractSelectStatement<String> createStatement(final Employee employee) {
        return employee.lastName.orderBy(employee.lastName).limit(2).forReadOnly();
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createStatement(employee).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First"), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> expected = new ArrayList<>(Arrays.asList("Cooper", "First"));
        createStatement(employee).scroll(getEngine(), new Callback<String>() {
            @Override
            public boolean iterate(final String s) throws SQLException {
                assertTrue(expected.toString(), !expected.isEmpty() && expected.remove(0).equals(s));
                return true;
            }
        });
        assertTrue(expected.toString(), expected.isEmpty());
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final AbstractSelectStatement<String> selectStatement = employee.lastName.forReadOnly();
        final String sql = selectStatement.showQuery(getEngine().getDialect());
        assertSimilar("SELECT T0.last_name AS C0 FROM employee AS T0 FOR READ ONLY", sql);

    }
}
