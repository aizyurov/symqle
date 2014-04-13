package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.sql.Label;
import org.symqle.testset.LabelTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class LabelTest extends AbstractIntegrationTestBase implements LabelTestSet {

    @Override
    public void test_asc_() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<String> list = employee.lastName
                .label(l)
                .where(employee.department().deptName.eq("DEV"))
                .orderBy(l.asc())
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Pedersen"), list);
    }

    @Override
    public void test_desc_() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<String> list = employee.lastName
                .label(l)
                .where(employee.department().deptName.eq("DEV"))
                .orderBy(l.desc())
                .list(getEngine());
        assertEquals(Arrays.asList("Pedersen", "First"), list);
    }

    @Override
    public void test_nullsFirst_() throws Exception {
        try {
            final Employee employee = new Employee();
            final Label l = new Label();
            final List<Pair<String,String>> list = employee.department().deptName
                    .label(l)
                    .pair(employee.lastName)
                    .where(employee.firstName.eq("James"))
                    .orderBy(l.nullsFirst())
                    .list(getEngine());
            assertEquals(Arrays.asList(Pair.make((String) null, "Cooper"), Pair.make("DEV", "First")), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        try {
            final Employee employee = new Employee();
            final Label l = new Label();
            final List<Pair<String,String>> list = employee.department().deptName
                    .label(l)
                    .pair(employee.lastName)
                    .where(employee.firstName.eq("James"))
                    .orderBy(l.nullsLast())
                    .list(getEngine());
            assertEquals(Arrays.asList(Pair.make("DEV", "First"), Pair.make((String) null, "Cooper")), list);
        } catch (SQLException e) {
            // mysql does not support NULLS LAST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final Employee employee = new Employee();
        final Label l = new Label();
        final List<String> list = employee.lastName
                .label(l)
                .where(employee.department().deptName.eq("DEV"))
                .orderBy(l)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Pedersen"), list);
    }
}
