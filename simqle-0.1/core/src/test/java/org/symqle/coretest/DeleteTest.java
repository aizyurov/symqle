package org.symqle.coretest;

import org.symqle.Mappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractDeleteStatement;
import org.symqle.sql.AbstractDeleteStatementBase;
import org.symqle.sql.Column;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

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
        new ExecuteScenario() {
            @Override
            protected void runExecute(final AbstractDeleteStatementBase update, final DatabaseGate gate) throws SQLException {
                assertEquals(2, update.execute(gate));
            }
        }.play();

    }

    private static abstract class ExecuteScenario {

        public void play() throws Exception {
            final AbstractDeleteStatementBase update = person.delete();
            final String statementString = update.show();
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(GenericDialect.get());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(statementString)).andReturn(statement);
            expect(statement.executeUpdate()).andReturn(2);
            statement.close();
            connection.close();
            replay(gate, connection,  statement);

            runExecute(update, gate);
            verify(gate, connection, statement);
        }

        protected abstract void runExecute(final AbstractDeleteStatementBase update, final DatabaseGate gate) throws SQLException;
    }

    public void testExecuteSearched() throws Exception {
        new ExecuteSearchedScenario() {
            @Override
            protected void runExecute(final AbstractDeleteStatement update, final DatabaseGate gate) throws SQLException {
                assertEquals(1, update.execute(gate));
            }
        }.play();

    }

    private static abstract class ExecuteSearchedScenario {

        public void play() throws Exception {
            final AbstractDeleteStatement update = person.delete().where(person.id.eq(1L));
            final String statementString = update.show();
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(GenericDialect.get());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(statementString)).andReturn(statement);
            statement.setLong(1, 1L);
            expect(statement.executeUpdate()).andReturn(1);
            statement.close();
            connection.close();
            replay(gate, connection,  statement);

            runExecute(update, gate);

            verify(gate, connection, statement);
        }

        protected abstract void runExecute(final AbstractDeleteStatement update, final DatabaseGate gate) throws SQLException;

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
