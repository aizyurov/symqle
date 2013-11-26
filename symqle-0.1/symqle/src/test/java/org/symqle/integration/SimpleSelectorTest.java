package org.symqle.integration;

import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.generic.AbstractSelector;
import org.symqle.integration.model.Employee;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author lvovich
 */
public class SimpleSelectorTest extends AbstractIntegrationTestBase {

    public void testSimpleListAll() throws Exception {
        final Employee employee = new Employee();
        final List<EmployeeDTO> list = new EmployeeSelector(employee).list(getEngine());
        final Set<EmployeeDTO> expected = new HashSet<EmployeeDTO>(Arrays.asList(
                new EmployeeDTO(1, "Margaret", "Redwood"),
                new EmployeeDTO(2, "Bill", "March"),
                new EmployeeDTO(3, "James", "First"),
                new EmployeeDTO(4, "Alex", "Pedersen"),
                new EmployeeDTO(5, "James", "Cooper")
        ));
        assertEquals(expected, new HashSet<EmployeeDTO>(list));
    }

    public void testOrderBy() throws Exception {
        final Employee employee = new Employee();
        final List<EmployeeDTO> list = new EmployeeSelector(employee).orderBy(employee.lastName).list(getEngine());
        final List<EmployeeDTO> expected = Arrays.asList(
                new EmployeeDTO(5, "James", "Cooper"),
                new EmployeeDTO(3, "James", "First"),
                new EmployeeDTO(2, "Bill", "March"),
                new EmployeeDTO(4, "Alex", "Pedersen"),
                new EmployeeDTO(1, "Margaret", "Redwood")
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
                new EmployeeDTO(3, "James", "First"),
                new EmployeeDTO(1, "Margaret", "Redwood")
        );
        assertEquals(expected, list);

    }


    private static class EmployeeDTO {
        private final Integer id;
        private final String firstName;
        private final String lastName;

        private EmployeeDTO(final Integer id, final String firstName, final String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final EmployeeDTO that = (EmployeeDTO) o;

            if (!firstName.equals(that.firstName)) return false;
            if (!id.equals(that.id)) return false;
            if (!lastName.equals(that.lastName)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + firstName.hashCode();
            result = 31 * result + lastName.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return id + ": " + firstName + ' ' + lastName;
        }
    }

    private class EmployeeSelector extends AbstractSelector<EmployeeDTO> {
        private final RowMapper<Integer> id;
        private final RowMapper<String> firstName;
        private final RowMapper<String> lastName;

        private EmployeeSelector(final Employee employee) {
            id = map(employee.empId);
            firstName = map(employee.firstName);
            lastName = map(employee.lastName);
        }

        @Override
        protected EmployeeDTO create(final Row row) throws SQLException {
            return new EmployeeDTO(
                    id.extract(row),
                    firstName.extract(row),
                    lastName.extract(row)
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
