package org.symqle.integration;

import org.symqle.common.Callback;
import org.symqle.dialect.MySqlDialect;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractCursorSpecification;
import org.symqle.sql.Dialect;

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
        final List<String> list = createCursorSpecificaton(employee).list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testForUpdate() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createCursorSpecificaton(employee).forUpdate().list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: ERROR 42Y90: FOR UPDATE is not permitted in this type of statement
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testForReadOnly() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = createCursorSpecificaton(employee).forReadOnly().list(getEngine());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            if (MySqlDialect.class.equals(getEngine().initialContext().get(Dialect.class).getClass())) {
                // should work with MySqlDialect
                throw e;
            } else {
                // mysql does not support FOR READ ONLY natively
                expectSQLException(e, "MySQL");
            }
        }
    }

    public void testScroll() throws Exception {
        final List<String> employees = new ArrayList<String>(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"));
        final Employee employee = new Employee();
        createCursorSpecificaton(employee).scroll(getEngine(), new Callback<String>() {
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
