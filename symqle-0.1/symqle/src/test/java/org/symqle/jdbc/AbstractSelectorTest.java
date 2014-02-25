package org.symqle.jdbc;

import junit.framework.TestCase;
import org.symqle.common.CoreMappers;
import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.sql.AbstractSelector;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class AbstractSelectorTest extends TestCase {

    public void testSimpleMapperSql() {
        final Person person = new Person();
        final PersonSelector mapper = new PersonSelector(person);
        final String queryString = mapper.show(new GenericDialect());
        System.out.println(queryString);
        assertEquals("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0", queryString);
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
        final Person person = new Person();
        final MapCallFromCreateSelector mapper = new MapCallFromCreateSelector(person);
        final String queryString = mapper.show(new GenericDialect());
        final DataSource dataSource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(dataSource.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("mock");
        connection.close();
        expect(dataSource.getConnection()).andReturn(connection);
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
        replay(dataSource, connection,  statement, resultSet, metaData);

        final Engine engine = new ConnectorEngine(dataSource, new GenericDialect());
        try {
            final List<PersonDTO> list = mapper.list(engine);
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            // Ok
        }
        verify(dataSource, connection, statement, resultSet, metaData);
    }

    public void testSimpleMapper() throws Exception {
        final Person person = new Person();
        final PersonSelector mapper = new PersonSelector(person);
        final String queryString = mapper.show(new GenericDialect());
        final DataSource gate = createMock(DataSource.class);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(gate.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("mock");
        connection.close();
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
        replay(gate, connection,  statement, resultSet, metaData);

        final Engine engine = new ConnectorEngine(gate);
        final List<PersonDTO> list = mapper.list(engine);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).id.longValue());
        assertEquals("Alex", list.get(0).name);
        verify(gate, connection, statement, resultSet, metaData);

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
            final PersonDTO personDTO = new PersonDTO(idKey.extract(row), nameKey.extract(row));
            return personDTO;
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
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

}
