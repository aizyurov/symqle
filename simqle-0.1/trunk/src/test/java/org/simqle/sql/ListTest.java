package org.simqle.sql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.easymock.EasyMock.*;
import static org.easymock.EasyMock.verify;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 02.01.2013
 * Time: 18:04:19
 * To change this template use File | Settings | File Templates.
 */
public class ListTest extends SqlTestCase {

    public void testList() throws Exception {
        final AbstractQuerySpecification<Long> querySpecification = person.id.queryValue().where(employee.name.isNotNull());
        final String queryString = querySpecification.show();                                                                                                final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
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
        replay(datasource, connection,  statement, resultSet);

        final List<Long> list = querySpecification.list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).longValue());
        verify(datasource, connection, statement, resultSet);
    }


    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
    }

    private static Person person = new Person();
    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
    }

    private static Employee employee = new Employee();

}
