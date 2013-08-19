package org.symqle.integration;

import org.symqle.common.Mappers;
import org.symqle.integration.model.Arithmetics;
import org.symqle.integration.model.Employee;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.symqle.generic.Functions.*;

/**
 * @author lvovich
 */
public class FunctionsTest extends AbstractIntegrationTestBase {

    public void testAbs() throws Exception {
        final Employee employee = new Employee();
        final List<Double> list = abs(employee.salary.opposite())
                .map(Mappers.DOUBLE)
                .where(employee.lastName.eq("Redwood"))
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(3000.0), list);
    }

    public void testMod() throws Exception {
        final Arithmetics arithmetics = new Arithmetics();
        final List<Number> list = mod(arithmetics.leftInt(), arithmetics.rightInt()).list(getDatabaseGate());
        assertEquals(1, list.size());
        assertEquals(1, list.get(0).intValue());
    }

    public void testLn() throws Exception {
        final Arithmetics arithmetics = new Arithmetics();
        final List<Number> list = ln(arithmetics.leftDouble()).list(getDatabaseGate());
        assertTrue(Math.abs(Math.log(11.0) - list.get(0).doubleValue()) < 1e-10);
    }

    public void testExp() throws Exception {
        final Arithmetics arithmetics = new Arithmetics();
        final List<Number> list = exp(arithmetics.rightDouble()).list(getDatabaseGate());
        assertTrue(Math.abs(Math.exp(2.0) - list.get(0).doubleValue()) < 1e-10);
    }

    public void testSqrt() throws Exception {
        final Arithmetics arithmetics = new Arithmetics();
        final List<Number> list = sqrt(arithmetics.leftDouble()).list(getDatabaseGate());
        assertTrue(Math.abs(Math.sqrt(11.0) - list.get(0).doubleValue()) < 1e-10);
    }

    public void testFloor() throws Exception {
        final Arithmetics arithmetics = new Arithmetics();
        final List<Integer> list = floor(arithmetics.leftDouble().div(arithmetics.rightDouble()))
                .map(Mappers.INTEGER)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(5), list);

    }

    public void testCeil() throws Exception {
        final Arithmetics arithmetics = new Arithmetics();
        final List<Integer> list = ceil(arithmetics.leftDouble().div(arithmetics.rightDouble()))
                .map(Mappers.INTEGER)
                .list(getDatabaseGate());
        assertEquals(Arrays.asList(6), list);
    }

    public void testPower() throws Exception {
        final Arithmetics arithmetics = new Arithmetics();
        try {
            final List<Double> list = power(arithmetics.leftDouble(), arithmetics.rightDouble())
                    .map(Mappers.DOUBLE)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList(121.0), list);
        } catch (SQLException e) {
            // Derby: ERROR 42Y03: 'POWER' is not recognized as a function or procedure.
            expectSQLException(e, "Apache Derby");
        }
    }

    public void testPower3() throws Exception {
        final Arithmetics arithmetics = new Arithmetics();
        try {
            final List<Double> list = power(arithmetics.leftDouble(), 3)
                    .map(Mappers.DOUBLE)
                    .list(getDatabaseGate());
            assertEquals(Arrays.asList(1331.0), list);
        } catch (SQLException e) {
            // Derby: ERROR 42Y03: 'POWER' is not recognized as a function or procedure.
            expectSQLException(e, "Apache Derby");
        }
    }

}
