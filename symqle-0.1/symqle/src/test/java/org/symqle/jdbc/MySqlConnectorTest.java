package org.symqle.jdbc;

import junit.framework.TestCase;

import javax.sql.DataSource;
import java.sql.Connection;
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
public class MySqlConnectorTest extends TestCase {

    public void testPipesAsConcatGlobal() throws Exception {
        checkGlobalSettingsWithPipesAsConcat("PIPES_AS_CONCAT");
        checkGlobalSettingsWithPipesAsConcat(" REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ANSI");
    }

    public void testPipesAsConcatSession() throws Exception {
        checkSessionSettingsWithPipesAsConcat("REAL_AS_FLOAT,ANSI_QUOTES");
    }

    private void checkSessionSettingsWithPipesAsConcat(String sqlMode) throws SQLException {
        final DataSource dataSource = createMock(DataSource.class);
        final Connector connector = new DataSourceConnector(dataSource);
        final MySqlConnector mySqlConnector = new MySqlConnector(connector);
        final Connection connection1 = createMock(Connection.class);

        expect(dataSource.getConnection()).andReturn(connection1);
        final PreparedStatement statement1 = createMock(PreparedStatement.class);
        expect(connection1.prepareStatement("select @@sql_mode")).andReturn(statement1);
        final ResultSet resultSet1 = createMock(ResultSet.class);
        expect(statement1.executeQuery()).andReturn(resultSet1);
        expect(resultSet1.next()).andReturn(true);
        expect(resultSet1.getString(1)).andReturn(sqlMode);
        resultSet1.close();
        statement1.close();
        final PreparedStatement statement2 = createMock(PreparedStatement.class);
        expect(connection1.prepareStatement("set session sql_mode=?")).andReturn(statement2);
        final String sessionMode = sqlMode + ",PIPES_AS_CONCAT";
        statement2.setString(1, sessionMode);
        expect(statement2.executeUpdate()).andReturn(0);
        statement2.close();
        connection1.close();

        final Connection connection2 = createMock(Connection.class);
        expect(dataSource.getConnection()).andReturn(connection2);
        final PreparedStatement statement3 = createMock(PreparedStatement.class);
        expect(connection2.prepareStatement("select @@sql_mode")).andReturn(statement3);
        final ResultSet resultSet3 = createMock(ResultSet.class);
        expect(statement3.executeQuery()).andReturn(resultSet3);
        expect(resultSet3.next()).andReturn(true);
        expect(resultSet3.getString(1)).andReturn(sessionMode);
        resultSet3.close();
        statement3.close();
        connection2.close();

        replay(dataSource, connection1, statement1, resultSet1, connection2, statement2, statement3, resultSet3);

        mySqlConnector.getConnection().close();
        mySqlConnector.getConnection().close();

        verify(dataSource, connection1, statement1, resultSet1, connection2, statement2, statement3, resultSet3);
    }

    private void checkGlobalSettingsWithPipesAsConcat(String sqlMode) throws SQLException {
        final DataSource dataSource = createMock(DataSource.class);
        final Connector connector = new DataSourceConnector(dataSource);
        final MySqlConnector mySqlConnector = new MySqlConnector(connector);
        final Connection connection1 = createMock(Connection.class);

        expect(dataSource.getConnection()).andReturn(connection1);
        final PreparedStatement statement1 = createMock(PreparedStatement.class);
        expect(connection1.prepareStatement("select @@sql_mode")).andReturn(statement1);
        final ResultSet resultSet1 = createMock(ResultSet.class);
        expect(statement1.executeQuery()).andReturn(resultSet1);
        expect(resultSet1.next()).andReturn(true);
        expect(resultSet1.getString(1)).andReturn(sqlMode);
        resultSet1.close();
        statement1.close();
        connection1.close();

        final Connection connection2 = createMock(Connection.class);
        expect(dataSource.getConnection()).andReturn(connection2);
        connection2.close();

        replay(dataSource, connection1, statement1, resultSet1, connection2);

        mySqlConnector.getConnection().close();
        mySqlConnector.getConnection().close();

        verify(dataSource, connection1, statement1, resultSet1, connection2);

    }
}
