package org.symqle.integration;

import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractInValueList;
import org.symqle.sql.InValueList;
import org.symqle.testset.AbstractInValueListTestSet;

import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class InValueListTest extends AbstractIntegrationTestBase implements AbstractInValueListTestSet {

    @Override
    public void test_adapt_InValueList() throws Exception {
        final Employee employee = new Employee();
        final InValueList<Integer> values = employee.department().manager().empId.asInValueList();
        final AbstractInValueList<Integer> adaptor = AbstractInValueList.adapt(values);
        final List<String> list = employee.lastName.where(employee.empId.in(adaptor))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_append_ValueExpression() throws Exception {
        final Employee employee = new Employee();
        final AbstractInValueList<Integer> managerId = employee.department().manager().empId.asInValueList();
        final AbstractInValueList<Integer> myAndManagerIds = managerId.append(employee.empId);
        final List<String> list = employee.lastName.where(employee.empId.in(myAndManagerIds))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    @Override
    public void test_in_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractInValueList<Integer> values = employee.department().manager().empId.asInValueList();
        final List<String> list = employee.lastName.where(employee.empId.in(values))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    @Override
    public void test_notIn_Predicand_InPredicateValue_1() throws Exception {
        final Employee employee = new Employee();
        final AbstractInValueList<Integer> values = employee.department().manager().empId.asInValueList();
        final List<String> list = employee.lastName.where(employee.empId.notIn(values))
                .orderBy(employee.lastName)
                .list(getEngine());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }
}
