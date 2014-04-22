package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;
import org.symqle.common.OnDemand;

/**
 * @author lvovich
 */
public class Department extends Table {


    @Override
    public String getTableName() {
        return"department";
    }

    public final Column<Integer> deptId = defineColumn(CoreMappers.INTEGER, "dept_id");
    public final Column<String> deptName = defineColumn(CoreMappers.STRING, "dept_name");
    public final Column<Integer> countryId = defineColumn(CoreMappers.INTEGER, "country_id");

    private final OnDemand<Country> countryRef = new OnDemand<Country>() {
        @Override
        protected Country construct() {
            final Country country = new Country();
            leftJoin(country, country.countryId.eq(countryId));
            return country;
        }
    };

    public Country country() {
        return countryRef.get();
    }

    public final Column<Integer> managerId = defineColumn(CoreMappers.INTEGER, "manager_id");

    private final OnDemand<Employee> managerRef = new OnDemand<Employee>() {
        @Override
        protected Employee construct() {
            final Employee manager = new Employee();
            leftJoin(manager, manager.empId.eq(managerId));
            return manager;
        }
    };

    public final Employee manager() {
        return managerRef.get();
    }

}
