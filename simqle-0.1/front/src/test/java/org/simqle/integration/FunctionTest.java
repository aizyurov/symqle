package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.integration.model.Employee;
import org.simqle.sql.AbstractNumericExpression;
import org.simqle.sql.AbstractRoutineInvocation;
import org.simqle.sql.SqlFunction;
import org.simqle.sql.ValueExpression;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class FunctionTest extends AbstractIntegrationTestBase {

    private <T> AbstractRoutineInvocation<T> abs(ValueExpression<T> e) {
        return SqlFunction.create("abs", e.getMapper()).apply(e);
    }

    public void testList() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

   public void testAll() throws Exception {
       final Employee employee = new Employee();
       final List<Double> list = abs(employee.salary.opposite()).all().list(getDialectDataSource());
       Collections.sort(list);
       assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

   public void testDistinct() throws Exception {
       final Employee employee = new Employee();
       final List<Double> list = abs(employee.salary.opposite()).distinct().list(getDialectDataSource());
       Collections.sort(list);
       assertEquals(Arrays.asList(1500.0, 2000.0, 3000.0), list);
   }

    public void testAsFunctionArgument() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(abs(employee.salary.opposite())).list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(1500.0, 2000.0, 2000.0, 3000.0, 3000.0), list);
    }

    public void testBooleanValue() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).booleanValue())
                    .orderBy(employee.lastName)
                    .list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
        } catch (SQLException e) {
            // derby: Cannot convert types 'DOUBLE' to 'BOOLEAN'.
            expectSQLException(e, "derby");
        }
    }

    public void testEq() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).eq(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testNe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).ne(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(), list);
    }

    public void testGt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).gt(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(), list);
    }

    public void testGe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).ge(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLt() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).lt(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(), list);
    }

    public void testLe() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).le(employee.salary))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "March", "Pedersen", "Redwood"), list);
    }

    public void testEqValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).eq(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("March", "Pedersen"), list);
    }

    public void testNeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).ne(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "First", "Redwood"), list);
    }

    public void testGtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).gt(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testGeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).ge(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testLtValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).lt(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testLeValue() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).le(2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testIn() throws Exception {
        final Employee employee = new Employee();
        final Employee hr = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).in(hr.salary.where(hr.department().deptName.eq("HR"))))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
    }

    public void testInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).in(1500.0, 2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen"), list);
    }

    public void testNotIn() throws Exception {
        final Employee employee = new Employee();
        final Employee hr = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).notIn(hr.salary.where(hr.department().deptName.eq("HR"))))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
    }

    public void testNotInList() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.salary.opposite()).notIn(1500.0, 2000.0))
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "Redwood"), list);
    }

    public void testIsNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.deptId).isNull())
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper"), list);
   }

    public void testIsNotNull() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.where(abs(employee.deptId).isNotNull())
                .orderBy(employee.lastName)
                .list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "March", "Pedersen", "Redwood"), list);
   }

    public void testAsSortSpec() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(abs(employee.salary.opposite()), employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
    }

    public void testOrderByNullsFirst() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(abs(employee.deptId).nullsFirst(), employee.lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList("Cooper", "March", "Redwood", "First", "Pedersen"), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testOrderByNullsLast() throws Exception {
        final Employee employee = new Employee();
        try {
            final List<String> list = employee.lastName.orderBy(abs(employee.deptId).nullsLast(), employee.lastName).list(getDialectDataSource());
            assertEquals(Arrays.asList("March", "Redwood", "First", "Pedersen", "Cooper"), list);
        } catch (SQLException e) {
            // mysql does not support NULLS FIRST
            expectSQLException(e, "mysql");
        }
    }

    public void testAsc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(abs(employee.salary.opposite()).asc(), employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("Cooper", "March", "Pedersen", "First", "Redwood"), list);
    }

    public void testDesc() throws Exception {
        final Employee employee = new Employee();
        final List<String> list = employee.lastName.orderBy(abs(employee.salary.opposite()).desc(), employee.lastName).list(getDialectDataSource());
        assertEquals(Arrays.asList("First", "Redwood", "March", "Pedersen", "Cooper"), list);
    }

    public void testOpposite() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite()).opposite().list(getDialectDataSource());
        Collections.sort(list);
        assertEquals(Arrays.asList(-3000.0, -3000.0, -2000.0, -2000.0, -1500.0), list);
    }

    public void testPair() throws Exception {
        final Employee employee = new Employee();
        final List<Pair<Double,String>> list = abs(employee.salary.opposite()).pair(employee.lastName)
                .where(employee.deptId.isNull())
                .list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.of(1500.0, "Cooper")), list);
    }

    public void testPlus() throws Exception {
        final Employee employee = new Employee();
        final AbstractNumericExpression<Number> sum = abs(employee.salary.opposite()).plus(employee.department().manager().salary);
        final List<Number> list = sum.orderBy(employee.lastName).list(getDialectDataSource());
        final List<Double> asDoubles = new ArrayList<Double>();
        for (Number n : list) {
            asDoubles.add(n == null ? null : n.doubleValue());
        }
        assertEquals(Arrays.asList(null, 6000.0, 5000.0, 5000.0, 6000.0), asDoubles);
    }
}
