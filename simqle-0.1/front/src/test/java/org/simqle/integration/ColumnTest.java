package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.integration.model.Country;
import org.simqle.integration.model.Department;
import org.simqle.integration.model.Employee;

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

    public void test() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<String,String>> developers = employee.firstName.pair(employee.lastName).where(employee.department().deptName.eq("DEV")).list(getDialectDataSource());
        System.out.println(developers);
    }
}
