package org.simqle.coretest;


import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractQuerySpecification;
import org.simqle.sql.Column;
import org.simqle.sql.DialectDataSource;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.GenericDialect;
import org.simqle.sql.TableOrView;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
        replay(datasource, connection, statement, resultSet);
        id.scroll(datasource, new Callback<Long>() {
            @Override
            public boolean iterate(Long aLong) {
                fail("Must not be called");
                return true;
            }
        });
        verify(datasource, statement, connection, resultSet);
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
        replay(datasource, connection, statement, resultSet);
        id.scroll(datasource, new Callback<Long>() {
            private int callCount = 0;

            @Override
            public boolean iterate(Long aLong) {
                if (callCount > 0) {
                    fail("Must not get here");
                } else {
                    callCount++;
                    assertEquals(123, aLong.longValue());
                }
                return true;
            }
        });
        verify(datasource, statement, connection, resultSet);
    }

    public void testScrollWithDialect() throws Exception {
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
        replay(datasource, connection, statement, resultSet);
        id.scroll(new DialectDataSource(GenericDialect.get(), datasource), new Callback<Long>() {
            private int callCount = 0;

            @Override
            public boolean iterate(Long aLong) {
                if (callCount > 0) {
                    fail("Must not get here");
                } else {
                    callCount++;
                    assertEquals(123, aLong.longValue());
                }
                return true;
            }
        });
        verify(datasource, statement, connection, resultSet);
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
        replay(datasource, connection, statement, resultSet);
        int callNum = id.scroll(datasource, new Callback<Long>() {
            private int callCount = 0;
            @Override
            public boolean iterate(Long aLong) {
                if (callCount > 0) {
                    assertEquals(254, aLong.longValue());
                    return false;
                } else {
                    callCount ++;
                    assertEquals(123, aLong.longValue());
                    return true;
                }
            }
        });
        assertEquals(2, callNum);
        verify(datasource, statement, connection, resultSet);
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
        replay(datasource, connection, statement, resultSet);
        final List<Long> list = id.list(datasource);
        assertEquals(1, list.size());
        assertEquals(Long.valueOf(123), list.get(0));
        verify(datasource, statement, connection, resultSet);

    }

    public void testListWithParameter() throws Exception {
        final Person person = new Person();
        final Column<Long> id = person.id;
        final DynamicParameter<Long> param = DynamicParameter.create(Mappers.LONG, 123L);
        final String queryString = id.where(id.eq(param)).show();
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
        replay(datasource, connection, statement, resultSet);
        final List<Long> list = id.where(id.eq(param)).list(datasource);
        assertEquals(1, list.size());
        assertEquals(Long.valueOf(123), list.get(0));
        verify(datasource, statement, connection, resultSet);
    }

    public void testListWithComplexCondition() throws Exception {
        final Person person = new Person();
        final AbstractQuerySpecification<Long> query = person.id.where(person.age.add(1).gt(33));
        final String queryString = query.show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setBigDecimal(1, new BigDecimal(1));
        statement.setBigDecimal(2, new BigDecimal(33));
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection, statement, resultSet);
        final List<Long> list = query.list(datasource);
        assertEquals(1, list.size());
        assertEquals(Long.valueOf(123), list.get(0));
        verify(datasource, statement,  connection, resultSet);
    }

    public void testListWithBooleanColumnCondition() throws Exception {
        final Person person = new Person();
        final AbstractQuerySpecification<Long> query = person.id.where(person.alive.eq(true));
        final String queryString = query.show();
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setBoolean(1, true);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection, statement, resultSet);
        final List<Long> list = query.list(datasource);
        assertEquals(1, list.size());
        assertEquals(Long.valueOf(123), list.get(0));
        verify(datasource, statement,  connection, resultSet);
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "id");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
        public Column<Boolean> alive = defineColumn(Mappers.BOOLEAN, "alive");
    }

}
