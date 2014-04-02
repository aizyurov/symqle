package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractQueryExpression;
import org.symqle.testset.AbstractQueryExpressionTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class QueryExpressionTest extends AbstractIntegrationTestBase implements AbstractQueryExpressionTestSet {

    private AbstractQueryExpression<String> createQueryExpression(final Employee employee) {
        return employee.lastName.orderBy(employee.lastName).limit(1);
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createQueryExpression(employee)
                .compileQuery(getEngine())
                .list();
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createQueryExpression(employee).forReadOnly().list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createQueryExpression(employee).forUpdate().list(getEngine());
            assertEquals(Arrays.asList("Cooper"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createQueryExpression(employee).list(getEngine());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final List<String> employees = new ArrayList<String>(Arrays.asList("Cooper"));
        final Employee employee = new Employee();
        final int count = createQueryExpression(employee).scroll(getEngine(), new Callback<String>() {
            @Override
            public boolean iterate(final String s) {
                assertTrue(employees + " does not contain " + s, employees.remove(s));
                return true;
            }
        });
        assertEquals(1, count);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = createQueryExpression(employee).showQuery(getEngine().getDialect());
        final Pattern expected;
        if (getDatabaseName().equals("MySQL")) {
            expected = Pattern.compile("SELECT ([A-Z][A-Z0-9]*).last_name AS [A-Z][A-Z0-9]* FROM employee AS \\1 ORDER BY \\1.last_name LIMIT 0, 1");
        } else {
            expected = Pattern.compile("SELECT ([A-Z][A-Z0-9]*).last_name AS [A-Z][A-Z0-9]* FROM employee AS \\1 ORDER BY \\1.last_name FETCH FIRST 1 ROWS ONLY");
        }
        assertTrue(sql, expected.matcher(sql).matches());
    }

}
