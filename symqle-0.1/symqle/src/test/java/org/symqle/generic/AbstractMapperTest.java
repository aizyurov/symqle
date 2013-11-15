package org.symqle.generic;

import junit.framework.TestCase;
import org.symqle.common.Mappers;
import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;

/**
 * @author lvovich
 */
public class AbstractMapperTest extends TestCase {

    public void testSimpleMapperSql() {
        final Person person = new Person();
        final PersonSelector mapper = new PersonSelector(person);
        final String queryString = mapper.show(new GenericDialect());
        System.out.println(queryString);
        assertEquals("SELECT T1.id AS C1, T1.name AS C2 FROM person AS T1", queryString);
    }

    public void testNoMappers() {
        final EmptyPersonSelector mapper = new EmptyPersonSelector();
        try {
            final String sql = mapper.show(new GenericDialect());
            fail("IllegalStateException expected but returned: "+sql);
        } catch (IllegalStateException e) {
            assertEquals("No mappings defined", e.getMessage());
        }
    }

    public void testMapFromCreate() throws Exception {
        fail("Not implemented");
//        final Person person = new Person();
//        final MapCallFromCreateSelector mapper = new MapCallFromCreateSelector(person);
//        final String queryString = mapper.show(new GenericDialect());
//        final DatabaseGate gate = createMock(DatabaseGate.class);
//        final Connection connection = createMock(Connection.class);
//        final PreparedStatement statement = createMock(PreparedStatement.class);
//        final ResultSet resultSet = createMock(ResultSet.class);
//        expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
//        expect(gate.initialContext().get(Dialect.class)).andReturn(new GenericDialect());
//        expect(gate.getConnection()).andReturn(connection);
//        expect(connection.prepareStatement(queryString)).andReturn(statement);
//        expect(statement.executeQuery()).andReturn(resultSet);
//        expect(resultSet.next()).andReturn(true);
//        // next 4 lines are not called because create() throws an exception
////        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
////        expect(resultSet.wasNull()).andReturn(false);
////        expect(resultSet.getString(matches("C[0-9]"))).andReturn("Alex");
////        expect(resultSet.next()).andReturn(false);
//        resultSet.close();
//        statement.close();
//        connection.close();
//        replay(gate, connection,  statement, resultSet);
//
//        try {
//            final List<PersonDTO> list = mapper.list(gate);
//            fail("IllegalStateException expected");
//        } catch (IllegalStateException e) {
//            // Ok
//        }
//        verify(gate, connection, statement, resultSet);
    }

    public void testSimpleMapper() throws Exception {
        fail("Not implemented");
//        final Person person = new Person();
//        final PersonSelector mapper = new PersonSelector(person);
//        final String queryString = mapper.show(new GenericDialect());
//        final DatabaseGate gate = createMock(DatabaseGate.class);
//        final Connection connection = createMock(Connection.class);
//        final PreparedStatement statement = createMock(PreparedStatement.class);
//        final ResultSet resultSet = createMock(ResultSet.class);
//        expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
//        expect(gate.initialContext().get(Dialect.class)).andReturn(new GenericDialect());
//        expect(gate.getConnection()).andReturn(connection);
//        expect(connection.prepareStatement(queryString)).andReturn(statement);
//        expect(statement.executeQuery()).andReturn(resultSet);
//        expect(resultSet.next()).andReturn(true);
//        expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
//        expect(resultSet.wasNull()).andReturn(false);
//        expect(resultSet.getString(matches("C[0-9]"))).andReturn("Alex");
//        expect(resultSet.next()).andReturn(false);
//        resultSet.close();
//        statement.close();
//        connection.close();
//        replay(gate, connection,  statement, resultSet);
//
//        final List<PersonDTO> list = mapper.list(gate);
//        assertEquals(1, list.size());
//        assertEquals(123L, list.get(0).id.longValue());
//        assertEquals("Alex", list.get(0).name);
//        verify(gate, connection, statement, resultSet);
//
    }

    private static class PersonDTO {
        private final Long id;
        private final String name;

        private PersonDTO(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }
    }

    private static class PersonSelector extends AbstractSelector<PersonDTO> {

        private final RowMapper<Long> idKey;
        private final RowMapper<String> nameKey;

        public PersonSelector(final Person person) {
            idKey = map(person.id);
            nameKey = map(person.name);
        }


        @Override
        protected PersonDTO create(final Row row) throws SQLException {
            return new PersonDTO(idKey.extract(row), nameKey.extract(row));
        }
    }

    private static class EmptyPersonSelector extends AbstractSelector<PersonDTO> {
        @Override
        protected PersonDTO create(final Row row) throws SQLException {
            return null;
        }
    }

    private static class MapCallFromCreateSelector extends AbstractSelector<PersonDTO> {
        private final Person person;

        private MapCallFromCreateSelector(final Person person) {
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
