package org.symqle.integration.model;

import org.symqle.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;
import org.symqle.util.LazyRef;

/**
 * @author lvovich
 */
public class Department extends Table {

    public Department() {
        super("department");
    }

    public final Column<Integer> deptId = defineColumn(Mappers.INTEGER, "dept_id");
    public final Column<String> deptName = defineColumn(Mappers.STRING, "dept_name");
    public final Column<Integer> countryId = defineColumn(Mappers.INTEGER, "country_id");

    private final LazyRef<Country> countryRef = new LazyRef<Country>() {
        @Override
        protected Country create() {
            final Country country = new Country();
            leftJoin(country, country.countryId.eq(countryId));
            return country;
        }
    };

    public Country country() {
        return countryRef.get();
    }

    public final Column<Integer> managerId = defineColumn(Mappers.INTEGER, "manager_id");

    private final LazyRef<Employee> managerRef = new LazyRef<Employee>() {
        @Override
        protected Employee create() {
            final Employee manager = new Employee();
            leftJoin(manager, manager.empId.eq(managerId));
            return manager;
        }
    };

    public final Employee manager() {
        return managerRef.get();
    }

}
