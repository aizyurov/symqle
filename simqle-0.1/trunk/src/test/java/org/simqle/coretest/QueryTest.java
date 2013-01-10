package org.simqle.coretest;


import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.TableOrView;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

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
        final Person person = new Person();
        final Column<Long> id = person.id;
        final String queryString = id.show();
        System.out.println("Show: " + queryString);
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
        final Person person = new Person();
        final Column<Long> id = person.id;
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

    public void testScrollWithBreak() throws Exception {
        final Person person = new Person();
        final Column<Long> id = person.id;
        final String queryString = id.show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(254L);
        expect(resultSet.wasNull()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replayAll();
        int callNum = id.scroll(datasource, new Callback<Long, SQLException>() {
            private int callCount = 0;
            @Override
            public void iterate(Long aLong) throws SQLException, BreakException {
                if (callCount > 0) {
                    assertEquals(254, aLong.longValue());
                    throw new BreakException();
                } else {
                    callCount ++;
                    assertEquals(123, aLong.longValue());
                }
            }
        });
        assertEquals(1, callNum);
    }

    public void testList() throws Exception {
        final Person person = new Person();
        final Column<Long> id = person.id;
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
        final List<Long> list = id.list(datasource);
        assertEquals(1, list.size());
        assertEquals(Long.valueOf(123), list.get(0));

    }

    public void testListWithParameter() throws Exception {
        final Person person = new Person();
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 123L);
        final String queryString = id.where(id.eq(param)).show();
        System.out.println(queryString);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setLong(1, 123L);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replayAll();
        final List<Long> list = id.where(id.eq(param)).list(datasource);
        assertEquals(1, list.size());
        assertEquals(Long.valueOf(123), list.get(0));

    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "id");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
    }

}
