package org.symqle.front;

import junit.framework.TestCase;
import org.symqle.Mappers;
import org.symqle.Row;
import org.symqle.RowMapper;
import org.symqle.jdbc.Option;
import org.symqle.sql.Column;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class AbstractMapperTest extends TestCase {

    public void testSimpleMapperSql() {
        final Person person = new Person();
        final PersonMapper mapper = new PersonMapper(person);
        final String queryString = mapper.show();
        System.out.println(queryString);
        assertEquals("SELECT T1.id AS C1, T1.name AS C2 FROM person AS T1", queryString);
    }

    public void testNoMappers() {
        final EmptyPersonMapper mapper = new EmptyPersonMapper();
        try {
            final String sql = mapper.show();
            fail("IllegalStateException expected but returned: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("No mappings defined", e.getMessage());
        }
    }

    public void testMapFromCreate() throws Exception {
        final Person person = new Person();
        final MapCallFromCreateMapper mapper = new MapCallFromCreateMapper(person);
        final String queryString = mapper.show();
        final DatabaseGate gate = createMock(DatabaseGate.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
        expect(gate.getDialect()).andReturn(GenericDialect.get());
        expect(gate.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        // next 4 lines are not called because create() throws an exception
//        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
//        expect(resultSet.wasNull()).andReturn(false);
//        expect(resultSet.getString(matches("C[0-9]"))).andReturn("Alex");
//        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(gate, connection,  statement, resultSet);

        try {
            final List<PersonDTO> list = mapper.list(gate);
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            // Ok
        }
        verify(gate, connection, statement, resultSet);
    }

    public void testSimpleMapper() throws Exception {
        final Person person = new Person();
        final PersonMapper mapper = new PersonMapper(person);
        final String queryString = mapper.show();
        final DatabaseGate gate = createMock(DatabaseGate.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
        expect(gate.getDialect()).andReturn(GenericDialect.get());
        expect(gate.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getString(matches("C[0-9]"))).andReturn("Alex");
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(gate, connection,  statement, resultSet);

        final List<PersonDTO> list = mapper.list(gate);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).id.longValue());
        assertEquals("Alex", list.get(0).name);
        verify(gate, connection, statement, resultSet);

    }

    private static class PersonDTO {
        private final Long id;
        private final String name;

        private PersonDTO(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }
    }

    private static class PersonMapper extends AbstractMapper<PersonDTO> {

        private final RowMapper<Long> idKey;
        private final RowMapper<String> nameKey;

        public PersonMapper(final Person person) {
            idKey = map(person.id);
            nameKey = map(person.name);
        }


        @Override
        protected PersonDTO create(final Row row) throws SQLException {
            return new PersonDTO(idKey.extract(row), nameKey.extract(row));
        }
    }

    private static class EmptyPersonMapper extends AbstractMapper<PersonDTO> {
        @Override
        protected PersonDTO create(final Row row) throws SQLException {
            return null;
        }
    }

    private static class MapCallFromCreateMapper extends AbstractMapper<PersonDTO> {
        private final Person person;

        private MapCallFromCreateMapper(final Person person) {
            this.person = person;
            map(person.id);
        }

        @Override
        protected PersonDTO create(final Row row) throws SQLException {
            return new PersonDTO(map(person.id).extract(row), map(person.name).extract(row));
        }

    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }


}
