package org.simqle.integration.model;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.Table;
import org.simqle.util.LazyRef;

import java.sql.Date;

/**
 * @author lvovich
 */
public class Employee extends Table {

    public Employee() {
        super("employee");
    }

    public final Column<Integer> empId = defineColumn(Mappers.INTEGER, "emp_id");
    public final Column<String> firstName = defineColumn(Mappers.STRING, "first_name");
    public final Column<String> lastName = defineColumn(Mappers.STRING, "last_name");
    public final Column<String> title = defineColumn(Mappers.STRING, "title");
    public final Column<Date> hireDate = defineColumn(Mappers.DATE, "date");
    public final Column<Boolean> retired = defineColumn(Mappers.BOOLEAN, "is_retired");
    public final Column<Double> salary = defineColumn(Mappers.DOUBLE, "salary");

    public final Column<Integer> deptId = defineColumn(Mappers.INTEGER, "dept_id");

    private LazyRef<Department> deptRef = new LazyRef<Department>() {
        @Override
        protected Department create() {
            final Department dept = new Department();
            leftJoin(dept, dept.deptId.eq(deptId));
            return dept;
        }
    };

    public Department department() {
        return deptRef.get();
    }


}
