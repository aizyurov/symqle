package org.symqle.jdbc;

import junit.framework.TestCase;
import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.Table;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class BatcherTest extends TestCase {

    public void testBatching() throws Exception {
        final DataSource dataSource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final DatabaseMetaData metaData = createMock(DatabaseMetaData.class);
        final PreparedStatement preparedStatement = createMock(PreparedStatement.class);

        expect(dataSource.getConnection()).andReturn(connection);
        expect(connection.getMetaData()).andReturn(metaData);
        expect(metaData.getDatabaseProductName()).andReturn("Apache Derby");
        connection.close();


        replay(dataSource,  connection, preparedStatement, metaData);

        Engine engine = new ConnectorEngine(dataSource);

        Person person = new Person();
        final DynamicParameter<Long> idParam = person.id.param();
        final DynamicParameter<String> nameParam = person.name.param();

        final PreparedUpdate preparedUpdate = person.insert(person.id.set(idParam).also(person.name.set(nameParam))).compileUpdate(engine);
        final Batcher batcher = engine.newBatcher(10);

        verify(dataSource, connection, preparedStatement, metaData);
        reset(dataSource, connection, preparedStatement, metaData);

        expect(dataSource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement("INSERT INTO person(id, name) VALUES(?, ?)")).andReturn(preparedStatement);
        preparedStatement.setLong(1, 1L);
        preparedStatement.setString(2, "one");
        preparedStatement.addBatch();
        preparedStatement.setLong(1, 2L);
        preparedStatement.setString(2, "two");
        preparedStatement.addBatch();
        expect(preparedStatement.executeBatch()).andReturn(new int[]{1, 1});
        preparedStatement.close();
        connection.close();

        replay(dataSource, connection, preparedStatement, metaData);

        idParam.setValue(1L);
        nameParam.setValue("one");
        assertEquals(0, preparedUpdate.submit(batcher).length);

        idParam.setValue(2L);
        nameParam.setValue("two");
        assertEquals(0, preparedUpdate.submit(batcher).length);

        assertEquals(2, batcher.flush().length);
        verify(dataSource, connection, preparedStatement, metaData);
    }

    private static class Person extends Table {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

}
