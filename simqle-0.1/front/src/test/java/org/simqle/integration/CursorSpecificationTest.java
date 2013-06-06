package org.simqle.integration;

import org.simqle.Callback;
import org.simqle.integration.model.Employee;
import org.simqle.mysql.MysqlDialect;
import org.simqle.sql.AbstractCursorSpecification;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class CursorSpecificationTest extends AbstractIntegrationTestBase {

    private AbstractCursorSpecification<String> createCursorSpecificaton(final Employee employee) {
        return employee.lastName.orderBy(employee.lastName);
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = createCursorSpecificaton(employee).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createCursorSpecificaton(employee).forUpdate().list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement
            expectSQLException(e, "derby");
        }
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createCursorSpecificaton(employee).forReadOnly().list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            if (MysqlDialect.class.equals(getDialectDataSource().getDialect().getClass())) {
                // should work with MysqlDialect
                throw e;
            } else {
                // mysql does not support FOR READ ONLY natively
                expectSQLException(e, "mysql");
            }
        }
    }

    public void testScroll() throws Exception {
        final List<String> employees = new ArrayList<String>(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"));
        final Employee employee = new Employee();
        createCursorSpecificaton(employee).scroll(getDialectDataSource(), new Callback<String>() {
            @Override
            public boolean iterate(final String s) {
                assertEquals(s, employees.get(0));
                employees.remove(s);
                return !s.equals("Pedersen");
            }
        });
        assertEquals(Arrays.asList("Redwood"), employees);
    }
}
