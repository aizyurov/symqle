package org.symqle.gate;

import junit.framework.TestCase;
import org.symqle.jdbc.Option;
import org.symqle.sql.Dialect;
import org.symqle.sql.GenericDialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;


/**
 * @author lvovich
 */
public class DialectDataSourceTest extends TestCase {

    public void testConnectionSetup() throws SQLException {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement setupStatement = createMock(PreparedStatement.class);
        final String connectionSetup = "set session sql_mode=?";
        final DataSourceGate dataSourceGate = new DataSourceGate(datasource);
        final PreparedStatement queryModeStatement = createMock(PreparedStatement.class);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final ResultSet resultSet = createMock(ResultSet.class);


        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("MySQL");
        connection.close();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement("select @@sql_mode")).andReturn(queryModeStatement);
        expect(queryModeStatement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(1)).andReturn("");
        resultSet.close();
        queryModeStatement.close();
        expect(connection.prepareStatement(connectionSetup)).andReturn(setupStatement);
        setupStatement.setString(1, "PIPES_AS_CONCAT");
        expect(setupStatement.executeUpdate()).andReturn(0);
        setupStatement.close();

        replay(datasource, connection,  setupStatement, queryModeStatement, metaData, resultSet);

        assertNotNull(dataSourceGate.getConnection());
        assertEquals("MySQL", dataSourceGate.getDialect().getName());

        verify(datasource, connection, setupStatement, queryModeStatement, metaData, resultSet);
    }

    public void testMysqlGlobalPipesAsConcat() throws SQLException {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement setupStatement = createMock(PreparedStatement.class);
        final String connectionSetup = "set session sql_mode=?";
        final DataSourceGate dataSourceGate = new DataSourceGate(datasource);
        final PreparedStatement queryModeStatement = createMock(PreparedStatement.class);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final ResultSet resultSet = createMock(ResultSet.class);


        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("MySQL");
        connection.close();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement("select @@sql_mode")).andReturn(queryModeStatement);
        expect(queryModeStatement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(1)).andReturn("PIPES_AS_CONCAT");
        resultSet.close();
        queryModeStatement.close();

        replay(datasource, connection,  setupStatement, queryModeStatement, metaData, resultSet);

        assertNotNull(dataSourceGate.getConnection());

        verify(datasource, connection, setupStatement, queryModeStatement, metaData, resultSet);
    }

    public void testMyqslSessionPipesAsConcatSet() throws SQLException {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement setupStatement = createMock(PreparedStatement.class);
        final String connectionSetup = "set session sql_mode=?";
        final DataSourceGate dataSourceGate = new DataSourceGate(datasource);
        final PreparedStatement queryModeStatement = createMock(PreparedStatement.class);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final ResultSet resultSet = createMock(ResultSet.class);


        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("MySQL");
        connection.close();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement("select @@sql_mode")).andReturn(queryModeStatement);
        expect(queryModeStatement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(1)).andReturn("");
        resultSet.close();
        queryModeStatement.close();
        expect(connection.prepareStatement(connectionSetup)).andReturn(setupStatement);
        setupStatement.setString(1, "PIPES_AS_CONCAT");
        expect(setupStatement.executeUpdate()).andReturn(0);
        setupStatement.close();

        // second call - PIPES_AS_CONCAT already set
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement("select @@sql_mode")).andReturn(queryModeStatement);
        expect(queryModeStatement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(1)).andReturn("PIPES_AS_CONCAT");
        resultSet.close();
        queryModeStatement.close();

        replay(datasource, connection,  setupStatement, queryModeStatement, metaData, resultSet);

        assertNotNull(dataSourceGate.getConnection());
        assertNotNull(dataSourceGate.getConnection());

        verify(datasource, connection, setupStatement, queryModeStatement, metaData, resultSet);

    }

    /**
     * Both at the first call and the second call session variable is unset
     * @throws SQLException
     */
    public void testMyqslSessionPipesAsConcatUnset() throws SQLException {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement setupStatement = createMock(PreparedStatement.class);
        final String connectionSetup = "set session sql_mode=?";
        final DataSourceGate dataSourceGate = new DataSourceGate(datasource);
        final PreparedStatement queryModeStatement = createMock(PreparedStatement.class);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final ResultSet resultSet = createMock(ResultSet.class);


        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("MySQL");
        connection.close();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement("select @@sql_mode")).andReturn(queryModeStatement);
        expect(queryModeStatement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(1)).andReturn("");
        resultSet.close();
        queryModeStatement.close();
        expect(connection.prepareStatement(connectionSetup)).andReturn(setupStatement);
        setupStatement.setString(1, "PIPES_AS_CONCAT");
        expect(setupStatement.executeUpdate()).andReturn(0);
        setupStatement.close();

        // second call - PIPES_AS_CONCAT not set
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement("select @@sql_mode")).andReturn(queryModeStatement);
        expect(queryModeStatement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(1)).andReturn("");
        resultSet.close();
        queryModeStatement.close();
        expect(connection.prepareStatement(connectionSetup)).andReturn(setupStatement);
        setupStatement.setString(1, "PIPES_AS_CONCAT");
        expect(setupStatement.executeUpdate()).andReturn(0);
        setupStatement.close();

        replay(datasource, connection,  setupStatement, queryModeStatement, metaData, resultSet);

        assertNotNull(dataSourceGate.getConnection());
        assertNotNull(dataSourceGate.getConnection());

        verify(datasource, connection, setupStatement, queryModeStatement, metaData, resultSet);

    }

    public void testMultipleSqlModeVariables() throws SQLException {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement setupStatement = createMock(PreparedStatement.class);
        final String connectionSetup = "set session sql_mode=?";
        final DataSourceGate dataSourceGate = new DataSourceGate(datasource);
        final PreparedStatement queryModeStatement = createMock(PreparedStatement.class);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final ResultSet resultSet = createMock(ResultSet.class);


        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("MySQL");
        connection.close();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement("select @@sql_mode")).andReturn(queryModeStatement);
        expect(queryModeStatement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(1)).andReturn("ONLY_FULL_GROUP_BY,REAL_AS_FLOAT");
        resultSet.close();
        queryModeStatement.close();
        expect(connection.prepareStatement(connectionSetup)).andReturn(setupStatement);
        setupStatement.setString(1, "ONLY_FULL_GROUP_BY,REAL_AS_FLOAT,PIPES_AS_CONCAT");
        expect(setupStatement.executeUpdate()).andReturn(0);
        setupStatement.close();

        // second call - PIPES_AS_CONCAT not set
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement("select @@sql_mode")).andReturn(queryModeStatement);
        expect(queryModeStatement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getString(1)).andReturn("ONLY_FULL_GROUP_BY,REAL_AS_FLOAT");
        resultSet.close();
        queryModeStatement.close();
        expect(connection.prepareStatement(connectionSetup)).andReturn(setupStatement);
        setupStatement.setString(1, "ONLY_FULL_GROUP_BY,REAL_AS_FLOAT,PIPES_AS_CONCAT");
        expect(setupStatement.executeUpdate()).andReturn(0);
        setupStatement.close();

        replay(datasource, connection,  setupStatement, queryModeStatement, metaData, resultSet);

        assertNotNull(dataSourceGate.getConnection());
        assertNotNull(dataSourceGate.getConnection());

        verify(datasource, connection, setupStatement, queryModeStatement, metaData, resultSet);
    }

    public void testUnknownDatabase() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final DataSourceGate dataSourceGate = new DataSourceGate(datasource);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("Unknown");
        connection.close();

        expect(datasource.getConnection()).andReturn(connection);
        expect(datasource.getConnection()).andReturn(connection);

        replay(datasource, connection, metaData, resultSet);

        assertNotNull(dataSourceGate.getConnection());
        assertNotNull(dataSourceGate.getConnection());

        assertEquals("Generic", dataSourceGate.getDialect().getName());

        verify(datasource, connection, metaData, resultSet);

    }

    public void testForceDialect() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final DataSourceGate dataSourceGate = new DataSourceGate(datasource, GenericDialect.get());
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("Apache Derby");
        connection.close();

        expect(datasource.getConnection()).andReturn(connection);
        expect(datasource.getConnection()).andReturn(connection);

        replay(datasource, connection, metaData, resultSet);

        assertNotNull(dataSourceGate.getConnection());
        assertNotNull(dataSourceGate.getConnection());

        assertEquals("Generic", dataSourceGate.getDialect().getName());

        verify(datasource, connection, metaData, resultSet);
    }

    public void testOptions() throws Exception {
            final DataSource datasource = createMock(DataSource.class);
            final Connection connection = createMock(Connection.class);
            final DataSourceGate dataSourceGate = new DataSourceGate(datasource, Option.setQueryTimeout(100), Option.setFetchSize(10));
            final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            expect(datasource.getConnection()).andReturn(connection);
            expect(connection.getMetaData()).andReturn(metaData);
            expect(metaData.getDatabaseProductName()).andReturn("Apache Derby");
            connection.close();

            expect(datasource.getConnection()).andReturn(connection);

            replay(datasource, connection, metaData, resultSet);

            assertNotNull(dataSourceGate.getConnection());

            assertEquals("Apache Derby", dataSourceGate.getDialect().getName());
            assertEquals(2, dataSourceGate.getOptions().size());

            verify(datasource, connection, metaData, resultSet);
    }

    public void testForceDialectAndOptions() throws Exception {
            final DataSource datasource = createMock(DataSource.class);
            final Connection connection = createMock(Connection.class);
            final DataSourceGate dataSourceGate = new DataSourceGate(datasource, GenericDialect.get(), Option.setQueryTimeout(100), Option.setFetchSize(10));
            final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            expect(datasource.getConnection()).andReturn(connection);
            expect(connection.getMetaData()).andReturn(metaData);
            expect(metaData.getDatabaseProductName()).andReturn("Apache Derby");
            connection.close();

            expect(datasource.getConnection()).andReturn(connection);

            replay(datasource, connection, metaData, resultSet);

            assertNotNull(dataSourceGate.getConnection());

            assertEquals("Generic", dataSourceGate.getDialect().getName());
            assertEquals(2, dataSourceGate.getOptions().size());

            verify(datasource, connection, metaData, resultSet);
    }

    public void testSQLException() throws Exception {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final DataSourceGate dataSourceGate = new DataSourceGate(datasource);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        final SQLException sqlException = new SQLException();
        expect(metaData.getDatabaseProductName()).andThrow(sqlException);
        connection.close();

        replay(datasource, connection, metaData);

        try {
            final Dialect dialect = dataSourceGate.getDialect();
            fail("IllegalStateException expected but returned " + dialect);
        } catch (IllegalStateException e) {
            assertEquals(sqlException, e.getCause());
        }

        verify(datasource, connection, metaData);
    }

}
