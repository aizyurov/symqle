package org.symqle.jdbc;

import junit.framework.TestCase;
import org.symqle.integration.model.Employee;
import org.symqle.sql.AbstractCursorSpecification;
import org.symqle.sql.GenericDialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class OptionTest extends TestCase {

    public void testOptions() throws Exception {
        Employee employee = new Employee();
        final AbstractCursorSpecification<String> cursorSpecification = employee.firstName.orderBy(employee.firstName);
        final String queryString = cursorSpecification.show(new GenericDialect());
        final DataSource connector = createMock(DataSource.class);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(connector.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("mock");
        connection.close();
        expect(connector.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        statement.setFetchSize(2);
        statement.setMaxFieldSize(5);
        statement.setMaxRows(10);
        statement.setQueryTimeout(7);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(matches("C[0-9]"))).andReturn("Alex");
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(connector, connection,  statement, resultSet, metaData);
        final Engine engine = new ConnectorEngine(connector);
        final List<String> list = cursorSpecification.list(engine,
                Option.setFetchDirection(ResultSet.FETCH_FORWARD),
                Option.setFetchSize(2),
                Option.setMaxFieldSize(5),
                Option.setMaxRows(10),
                Option.setQueryTimeout(7));
        assertEquals(Arrays.asList("Alex"), list);
        verify(connector, connection, statement, resultSet, metaData);
    }

    public void testEngineOptions() throws Exception {
        Employee employee = new Employee();
        final AbstractCursorSpecification<String> cursorSpecification = employee.firstName.orderBy(employee.firstName);
        final String queryString = cursorSpecification.show(new GenericDialect());
        final DataSource connector = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        expect(connector.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("mock");
        connection.close();
        expect(connector.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        statement.setFetchSize(2);
        statement.setMaxFieldSize(5);
        statement.setMaxRows(10);
        statement.setQueryTimeout(7);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(matches("C[0-9]"))).andReturn("Alex");
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(connector, connection,  statement, resultSet, metaData);
        final Engine engine = new ConnectorEngine(connector,
                Option.setFetchDirection(ResultSet.FETCH_FORWARD),
                        Option.setFetchSize(2),
                        Option.setMaxFieldSize(5),
                        Option.setMaxRows(10),
                        Option.setQueryTimeout(7));
        final List<String> list = cursorSpecification.list(engine
                );
        assertEquals(Arrays.asList("Alex"), list);
        verify(connector, connection, statement, resultSet, metaData);
    }
}
