package org.simqle;

import junit.framework.TestCase;
import org.simqle.front.StatementOptions;
import org.simqle.integration.model.Employee;
import org.simqle.sql.AbstractCursorSpecification;
import org.simqle.sql.DatabaseGate;
import org.simqle.sql.GenericDialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.matches;
import static org.easymock.EasyMock.replay;

/**
 * @author lvovich
 */
public class StatementOptionsTest extends TestCase {

    public void testOptions() throws Exception {
        Employee employee = new Employee();
        final AbstractCursorSpecification<String> cursorSpecification = employee.firstName.orderBy(employee.firstName);
        final String queryString = cursorSpecification.show();
        final DatabaseGate datasource = createMock(DatabaseGate.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getDialect()).andReturn(GenericDialect.get());
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        statement.setFetchSize(2);
        statement.setMaxFieldSize(5);
        statement.setFetchSize(10);
        statement.setMaxRows(10);
        statement.setQueryTimeout(7);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(matches("C[0-9]"))).andReturn("Alex");
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);
        final List<String> list = cursorSpecification.list(datasource,
                StatementOptions.setFetchDirection(ResultSet.FETCH_FORWARD),
                StatementOptions.setFetchSize(2),
                StatementOptions.setMaxFieldSize(5),
                StatementOptions.setMaxRows(10),
                StatementOptions.setQueryTimeout(7));
        assertEquals(Arrays.asList("Alex"), list);
    }
}
