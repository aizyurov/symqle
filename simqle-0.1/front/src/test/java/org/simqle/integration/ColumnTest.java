package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.integration.model.Country;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;
import org.simqle.sql.AbstractSelectList;

import java.util.List;

/**
 * @author lvovich
 */
public class ColumnTest extends AbstractIntegrationTestBase {

    private Integer usaCountryId;
    private Integer devDeptId;

    public ColumnTest() throws Exception {
    }

    public void onSetup() throws Exception {
        final Country country = new Country();
        usaCountryId = country.countryId.where(country.code.eq("USA")).list(getDialectDataSource()).get(0);
        final Department dept = new Department();
        devDeptId = dept.deptId.where(dept.deptName.eq("DEV")).list(getDialectDataSource()).get(0);
    }

    public void _testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> developers = employee.firstName.pair(employee.lastName).where(employee.department().deptName.eq("DEV")).list(getDialectDataSource());
        System.out.println(developers);
    }

    public void _testSubqueryInSelect() throws Exception {
        final Employee employee = new Employee();
        final Employee manager = new Employee();
        final List<Pair<String, String>> pairs = employee.lastName
                .pair(
                        manager.lastName.where(employee.department().deptId.eq(manager.department().deptId)
                                .and(manager.title.like("%manager"))).queryValue()
                )
                .where(employee.title.eq("guru")).list(getDialectDataSource());
        assertEquals(1, pairs.size());
        assertEquals(Pair.of("Pedersen", "First"), pairs.get(0));
    }

    public void testMultipleJoins() throws Exception {
        final Employee employee = new Employee();
        final AbstractSelectList<Pair<String,String>> select = employee.department().country().code.pair(employee.department().manager().department().country().code);
        System.out.println(select.show());
        final List<Pair<String, String>> countryCodes = select.list(getDialectDataSource());
//        final Connection connection = getDialectDataSource().getDataSource().getConnection();
//        connection.prepareStatement("SELECT T3.code AS C1 FROM employee AS T1 LEFT JOIN  department AS T2 LEFT JOIN country AS T3 ON T3.country_id = T2.country_id ON T2.dept_id = T1.dept_id");

        assertEquals(5, countryCodes.size());
        for (Pair<String, String> pair : countryCodes) {
            assertEquals(pair.getFirst(), pair.getSecond());
        }
    }
}
