package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.AbstractDeleteStatement;
import org.simqle.sql.AbstractDeleteStatementBase;
import org.simqle.sql.Column;
import org.simqle.sql.DialectDataSource;
import org.simqle.sql.GenericDialect;
import org.simqle.sql.Table;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class DeleteTest extends SqlTestCase {

    public void testDeleteAll() throws Exception {
        final AbstractDeleteStatementBase deleteStatementBase = person.delete();
        final String sql = deleteStatementBase.show();
        assertSimilar("DELETE FROM person", sql);
        final String sql2 = deleteStatementBase.show(GenericDialect.get());
        assertSimilar(sql, sql2);
    }

    public void testWhere() throws Exception {
        final AbstractDeleteStatement deleteStatement = person.delete().where(person.id.eq(1L));
        final String sql = deleteStatement.show();
        assertSimilar("DELETE FROM person WHERE person.id = ?", sql);
        assertSimilar(sql, deleteStatement.show(GenericDialect.get()));
    }

    public void testSubqueryInWhere() throws Exception {
        final Person child = new Person();
        final String sql = person.delete().where(child.id.where(child.parentId.eq(person.id)).exists()).show();
        assertSimilar("DELETE FROM person WHERE EXISTS(SELECT T0.id FROM person AS T0 WHERE T0.parent_id = person.id)", sql);
    }

    public void testWrongCondition() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.delete().where(child.name.eq("John")).show();
            fail("IllegalArgumentException expected, but was " + sql);
        } catch (IllegalArgumentException e) {
            // fine
            assertTrue(e.getMessage().contains("is not legal in this context"));
        }
    }

    public void testExecute() throws Exception {
        final AbstractDeleteStatementBase update = person.delete();
        final String statementString = update.show();
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(statementString)).andReturn(statement);
        expect(statement.executeUpdate()).andReturn(2);
        statement.close();
        connection.close();
        replay(datasource, connection,  statement);

        assertEquals(2, update.execute(datasource));
        verify(datasource, connection, statement);

        reset(datasource, connection, statement);

        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(statementString)).andReturn(statement);
        expect(statement.executeUpdate()).andReturn(2);
        statement.close();
        connection.close();
        replay(datasource, connection,  statement);

        assertEquals(2, update.execute(new DialectDataSource(GenericDialect.get(), datasource)));
        verify(datasource, connection, statement);
    }

    public void testExecuteSearched() throws Exception {
        final AbstractDeleteStatement update = person.delete().where(person.id.eq(1L));
        final String statementString = update.show();
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(statementString)).andReturn(statement);
        statement.setLong(1, 1L);
        expect(statement.executeUpdate()).andReturn(1);
        statement.close();
        connection.close();
        replay(datasource, connection,  statement);

        assertEquals(1, update.execute(datasource));

    }


    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();


}
