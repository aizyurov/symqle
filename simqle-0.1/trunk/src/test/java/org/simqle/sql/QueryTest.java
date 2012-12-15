package org.simqle.sql;


import org.simqle.Callback;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.easymock.EasyMock.*;
/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 15.12.2012
 * Time: 14:18:30
 * To change this template use File | Settings | File Templates.
 */
public class QueryTest extends SqlTestCase {
    private DataSource datasource = createMock(DataSource.class);
    private Connection connection = createMock(Connection.class);
    private PreparedStatement statement = createMock(PreparedStatement.class);
    private ResultSet resultSet = createMock(ResultSet.class);

    @Override
    protected void setUp() throws Exception {
        reset(datasource);
        reset(connection);
        reset(statement);
        reset(resultSet);
    }

    private void replayAll() {
        replay(datasource);
        replay(connection);
        replay(statement);
        replay(resultSet);
    }

    @Override
    protected void tearDown() throws Exception {
        verify(datasource);
        verify(connection);
        verify(statement);
        verify(resultSet);
    }

    public void testScrollWithEmptyResultSet() throws Exception {
        Table person = new Table("person");
        final LongColumn id = new LongColumn("id", person);
        final String queryString = id.show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replayAll();
        id.scroll(datasource, new Callback<Long, SQLException>() {
            @Override
            public void iterate(Long aLong) throws SQLException, BreakException {
                fail("Must not be called");
            }
        });
    }

    public void testScroll() throws Exception {
        Table person = new Table("person");
        final LongColumn id = new LongColumn("id", person);
        final String queryString = id.show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replayAll();
        id.scroll(datasource, new Callback<Long, SQLException>() {
            private int callCount = 0;
            @Override
            public void iterate(Long aLong) throws SQLException, BreakException {
                if (callCount > 0) {
                    fail("Must not get here");
                } else {
                    callCount ++;
                    assertEquals(123, aLong.longValue());
                }
            }
        });
    }
}
