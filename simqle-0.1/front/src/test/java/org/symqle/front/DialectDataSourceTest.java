package org.symqle.front;

import junit.framework.TestCase;

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
        final DialectDataSource dialectDataSource = new DialectDataSource(datasource);
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

        assertNotNull(dialectDataSource.getConnection());

        verify(datasource, connection, setupStatement, queryModeStatement, metaData, resultSet);

    }

}
