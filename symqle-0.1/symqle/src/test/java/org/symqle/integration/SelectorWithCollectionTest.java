package org.symqle.integration;

import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.integration.model.Department;
import org.symqle.integration.model.Employee;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractSelector;
import org.symqle.sql.SmartSelector;
import org.symqle.sql.DebugDialect;

import java.sql.SQLException;
import java.util.List;

/**
 * @author lvovich
 */
public class SelectorWithCollectionTest extends AbstractSelectorTestBase {

    public void testSimpleSelector() throws Exception {
        Department department = new Department();
        final List<DepartmentWithEmployeesDTO> list = new DepartmentSelector(department).orderBy(department.deptName).list(getEngine());
        assertEquals(2, list.size());
        final DepartmentWithEmployeesDTO dept1 = list.get(1);
        assertEquals("HR", dept1.name);
        assertEquals(2, dept1.employees.size());
        assertEquals("March", dept1.employees.get(0).getLastName());
        assertEquals("Redwood", dept1.employees.get(1).getLastName());
    }

    public void testSmartSelector() throws Exception {
        Department department = new Department();
        final List<DepartmentWithEmployeesDTO> list = new SmartDepartmentSelector(department).orderBy(department.deptName).list(getEngine());
        assertEquals(2, list.size());
        final DepartmentWithEmployeesDTO dept1 = list.get(1);
        assertEquals("HR", dept1.name);
        assertEquals(2, dept1.employees.size());
        assertEquals("March", dept1.employees.get(0).getLastName());
        assertEquals("Redwood", dept1.employees.get(1).getLastName());
    }

    public static class DepartmentWithEmployeesDTO {
        private final int id;
        private final String name;
        private final List<EmployeeDTO> employees;

        public DepartmentWithEmployeesDTO(final int id, final String name, final List<EmployeeDTO> employees) {
            this.id = id;
            this.name = name;
            this.employees = employees;
        }
    }

    private static class DepartmentSelector extends AbstractSelector<DepartmentWithEmployeesDTO> {
        private final RowMapper<Integer> idMapper;
        private final RowMapper<String> nameMapper;

        private DepartmentSelector(final Department department) {
            idMapper = map(department.deptId);
            nameMapper = map(department.deptName);
        }

        @Override
        protected DepartmentWithEmployeesDTO create(final Row row) throws SQLException {
            final Integer id = idMapper.extract(row);
            final String name = nameMapper.extract(row);
            final Employee employee = new Employee();
            final List<EmployeeDTO> employees = new EmployeeSelector(employee)
                    .where(employee.deptId.eq(id))
                    .orderBy(employee.lastName)
                    .list(row.getQueryEngine());
            return new DepartmentWithEmployeesDTO(id, name, employees);
        }
    }

    private class SmartDepartmentSelector extends SmartSelector<DepartmentWithEmployeesDTO> {
        private final Department department;

        private SmartDepartmentSelector(final Department department) {
            this.department = department;
        }

        @Override
        protected DepartmentWithEmployeesDTO create(final RowMap rowMap) throws SQLException {
            final QueryEngine queryEngine = rowMap.getQueryEngine();
            assertTrue(queryEngine.getDialect().getClass().equals(DebugDialect.class) || queryEngine.getDatabaseName().equals(getDatabaseName()));
            final Integer id = rowMap.get(department.deptId);
            final Employee employee = new Employee();
            final List<EmployeeDTO> employees = new EmployeeSelector(employee)
                    .where(employee.deptId.eq(id))
                    .orderBy(employee.lastName)
                    .list(queryEngine);
            return new DepartmentWithEmployeesDTO(id, rowMap.get(department.deptName), employees);
        }
    }

    private static class EmployeeSelector extends AbstractSelector<EmployeeDTO> {
        private final RowMapper<Integer> id;
        private final RowMapper<String> firstName;
        private final RowMapper<String> lastName;
        private final RowMapper<Integer> count;

        private EmployeeSelector(final Employee employee) {
            id = map(employee.empId);
            firstName = map(employee.firstName);
            lastName = map(employee.lastName);
            final Employee other = new Employee();
            count = map(other.empId.count().queryValue());
        }

        @Override
        protected EmployeeDTO create(final Row row) throws SQLException {
            return new EmployeeDTO(
                    id.extract(row),
                    firstName.extract(row),
                    lastName.extract(row),
                    count.extract(row)
            );
        }
    }

}
