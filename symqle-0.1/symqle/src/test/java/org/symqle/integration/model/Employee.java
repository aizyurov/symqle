package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;
import org.symqle.util.LazyRef;

import java.sql.Date;

/**
 * @author lvovich
 */
public class Employee extends Table {

    public Employee() {
        super("employee");
    }

    public final Column<Integer> empId = defineColumn(CoreMappers.INTEGER, "emp_id");
    public final Column<String> firstName = defineColumn(CoreMappers.STRING, "first_name");
    public final Column<String> lastName = defineColumn(CoreMappers.STRING, "last_name");
    public final Column<String> title = defineColumn(CoreMappers.STRING, "title");
    public final Column<Date> hireDate = defineColumn(CoreMappers.DATE, "hire_date");
    public final Column<Boolean> retired = defineColumn(CoreMappers.BOOLEAN, "is_retired");
    public final Column<Double> salary = defineColumn(CoreMappers.DOUBLE, "salary");

    public final Column<Integer> deptId = defineColumn(CoreMappers.INTEGER, "dept_id");

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
