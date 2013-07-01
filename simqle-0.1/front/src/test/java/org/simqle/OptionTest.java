package org.simqle;

import junit.framework.TestCase;
import org.simqle.integration.model.Employee;
import org.simqle.jdbc.Option;
import org.simqle.sql.AbstractCursorSpecification;
import org.simqle.sql.DatabaseGate;
import org.simqle.sql.GenericDialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.matches;
import static org.easymock.EasyMock.replay;

/**
 * @author lvovich
 */
public class OptionTest extends TestCase {

    public void testOptions() throws Exception {
        Employee employee = new Employee();
        final AbstractCursorSpecification<String> cursorSpecification = employee.firstName.orderBy(employee.firstName);
        final String queryString = cursorSpecification.show();
        final DatabaseGate datasource = createMock(DatabaseGate.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getOptions()).andReturn(Collections.<Option>emptyList());
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
                Option.setFetchDirection(ResultSet.FETCH_FORWARD),
                Option.setFetchSize(2),
                Option.setMaxFieldSize(5),
                Option.setMaxRows(10),
                Option.setQueryTimeout(7));
        assertEquals(Arrays.asList("Alex"), list);
    }
}
