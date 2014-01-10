package org.symqle.integration;

/**
 * @author lvovich
 */
public abstract class AbstractSelectorTestBase extends AbstractIntegrationTestBase {
    protected static class EmployeeDTO {
        private final Integer id;
        private final String firstName;
        private final String lastName;
        private final int total;

        protected EmployeeDTO(final Integer id, final String firstName, final String lastName, final int total) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.total = total;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final EmployeeDTO that = (EmployeeDTO) o;

            if (!firstName.equals(that.firstName)) return false;
            if (!id.equals(that.id)) return false;
            if (!lastName.equals(that.lastName)) return false;
            if (total != that.total) return false;

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
}
