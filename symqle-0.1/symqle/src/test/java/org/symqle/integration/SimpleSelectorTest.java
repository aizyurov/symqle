package org.symqle.integration;

import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.sql.AbstractSelector;
import org.symqle.integration.model.Employee;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author lvovich
 */
public class SimpleSelectorTest extends AbstractSelectorTestBase {

    public void testSimpleListAll() throws Exception {
        final Employee employee = new Employee();
        final List<EmployeeDTO> list = new EmployeeSelector(employee).list(getEngine());
        final Set<EmployeeDTO> expected = new HashSet<EmployeeDTO>(Arrays.asList(
                new EmployeeDTO(1, "Margaret", "Redwood", 5),
                new EmployeeDTO(2, "Bill", "March", 5),
                new EmployeeDTO(3, "James", "First", 5),
                new EmployeeDTO(4, "Alex", "Pedersen", 5),
                new EmployeeDTO(5, "James", "Cooper", 5)
        ));
        assertEquals(expected, new HashSet<EmployeeDTO>(list));
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<EmployeeDTO> list = new EmployeeSelector(employee).orderBy(employee.lastName).list(getEngine());
        final List<EmployeeDTO> expected = Arrays.asList(
                new EmployeeDTO(5, "James", "Cooper", 5),
                new EmployeeDTO(3, "James", "First", 5),
                new EmployeeDTO(2, "Bill", "March", 5),
                new EmployeeDTO(4, "Alex", "Pedersen", 5),
                new EmployeeDTO(1, "Margaret", "Redwood", 5)
        );
        assertEquals(expected, list);
    }

    public void testWhere() throws Exception {
        final Employee employee = new Employee();
        final List<EmployeeDTO> list = new EmployeeSelector(employee).
                where(employee.salary.ge(2800.0)).
                orderBy(employee.lastName).
                list(getEngine());
        final List<EmployeeDTO> expected = Arrays.asList(
                new EmployeeDTO(3, "James", "First", 5),
                new EmployeeDTO(1, "Margaret", "Redwood", 5)
        );
        assertEquals(expected, list);

    }


    private class EmployeeSelector extends AbstractSelector<EmployeeDTO> {
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

/*
|    101 | Margaret   | Redwood   | HR Manager          | 2008-08-22 |          0 |   3000 |       1 |
|    102 | Bill       | March     | HR Clerk            | 2008-08-23 |          0 |   2000 |       1 |
|    201 | James      | First     | Development manager | 2008-10-01 |          0 |   3000 |       2 |
|    202 | Alex       | Pedersen  | guru                | 2008-10-11 |          0 |   2000 |       2 |
|    203 | James      | Cooper    | hacker              | 2009-01-12 |          1 |   1500 |    NULL |

 */
