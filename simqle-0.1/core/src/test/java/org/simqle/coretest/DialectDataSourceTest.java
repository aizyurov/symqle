package org.simqle.coretest;

import junit.framework.TestCase;
import org.simqle.sql.DialectDataSource;
import org.simqle.sql.GenericDialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

/**
 * @author lvovich
 */
public class DialectDataSourceTest extends TestCase {

    public void testConnectionSetup() throws SQLException {
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final Statement statement = createMock(Statement.class);
        final String connectionSetup = "set session sql_mode='PIPES_AS_CONCAT'";
        final DialectDataSource dialectDataSource = new DialectDataSource(GenericDialect.get(), datasource, connectionSetup);

        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.createStatement()).andReturn(statement);
        expect(statement.executeUpdate(connectionSetup)).andReturn(0);
        statement.close();

        replay(datasource, connection,  statement);

        assertNotNull(dialectDataSource.getConnection());


    }
}
