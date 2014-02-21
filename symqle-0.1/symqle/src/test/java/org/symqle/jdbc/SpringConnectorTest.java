package org.symqle.jdbc;

import org.symqle.common.Pair;
import org.symqle.integration.AbstractIntegrationTestBase;
import org.symqle.integration.model.Employee;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * @author lvovich
 */
public class SpringConnectorTest extends AbstractIntegrationTestBase {

    @Override
    protected Engine createTestEngine(final DataSource dataSource) {
        try {
            return new SpringEngine(dataSource);
        } catch (Exception e) {
            throw new RuntimeException("Internal error", e);
        }
    }

    public void testSpringConnector() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> developers = employee.firstName.pair(employee.lastName).where(employee.department().deptName.eq("DEV")).list(getEngine());
        assertEquals(new HashSet<Pair<String,String>>(Arrays.asList(Pair.make("James", "First"), Pair.make("Alex", "Pedersen"))), new HashSet<Pair<String,String>>(developers));

    }
}
