package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractQueryExpressionBasic;
import org.symqle.testset.AbstractQueryExpressionBasicTestSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public class QueryExpressionBasicTest extends AbstractIntegrationTestBase implements AbstractQueryExpressionBasicTestSet {

    private AbstractQueryExpressionBasic<String> createQueryExpressionBasic(final Employee employee) {
        return employee.lastName.orderBy(employee.lastName);
    }

    @Override
    public void test_compileQuery_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createQueryExpressionBasic(employee)
                .compileQuery(getEngine())
                .list();
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_forReadOnly_() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createQueryExpressionBasic(employee).forReadOnly().list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_forUpdate_() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createQueryExpressionBasic(employee).forUpdate().list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement
            expectSQLException(e, "Apache Derby");
        }
    }

    @Override
    public void test_list_QueryEngine_Option() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createQueryExpressionBasic(employee).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_scroll_QueryEngine_Callback_Option() throws Exception {
        final List<String> employees = new ArrayList<String>(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"));
        final Employee employee = new Employee();
        createQueryExpressionBasic(employee).scroll(getEngine(), new Callback<String>() {
            @Override
            public boolean iterate(final String s) {
                assertEquals(s, employees.get(0));
                employees.remove(s);
                return !s.equals("Pedersen");
            }
        });
        assertEquals(Arrays.asList("Redwood"), employees);
    }

    @Override
    public void test_showQuery_Dialect_Option() throws Exception {
        final Employee employee = new Employee();
        final String sql = createQueryExpressionBasic(employee).showQuery(getEngine().getDialect());
        Pattern expected = Pattern.compile("SELECT ([A-Z][A-Z0-9]*).last_name AS [A-Z][A-Z0-9]* FROM employee AS \\1 ORDER BY \\1.last_name");
        assertTrue(sql, expected.matcher(sql).matches());
    }

}
