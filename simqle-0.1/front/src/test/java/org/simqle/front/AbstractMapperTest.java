package org.simqle.front;

import junit.framework.TestCase;
import org.simqle.Mappers;
import org.simqle.Row;
import org.simqle.sql.Column;
import org.simqle.sql.TableOrView;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    public void testSimpleMapper() throws Exception {
        final Person person = new Person();
        final PersonMapper mapper = new PersonMapper(person);
        final String queryString = mapper.show();
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getConnection()).andReturn(connection);
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
        replay(datasource, connection,  statement, resultSet);

        final List<PersonDTO> list = mapper.list(datasource);
        assertEquals(1, list.size());
        assertEquals(123L, list.get(0).id.longValue());
        assertEquals("Alex", list.get(0).name);
        verify(datasource, connection, statement, resultSet);

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

        private final Key<Long> idKey;
        private final Key<String> nameKey;

        public PersonMapper(final Person person) {
            idKey = key(person.id);
            nameKey = key(person.name);
        }


        @Override
        protected PersonDTO create(final Row row) throws SQLException {
            return new PersonDTO(idKey.value(row), nameKey.value(row));
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
