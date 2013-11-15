package org.symqle.coretest;

import org.symqle.common.MalformedStatementException;
import org.symqle.common.Mappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractUpdateStatement;
import org.symqle.sql.AbstractUpdateStatementBase;
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
public class UpdateTest extends SqlTestCase {

    public void testOneColumn() throws Exception {
        final String sql = person.update(person.parentId.set(person.id)).show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = person.id", sql);
        final String sql2 = person.update(person.parentId.set(person.id)).show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testSetNull() throws Exception {
        final String sql = person.update(person.parentId.setNull()).show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = NULL", sql);
    }

    public void testSetDefault() throws Exception {
        final String sql = person.update(person.parentId.setDefault()).show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = DEFAULT", sql);
    }

    public void testSetOverrideType() throws Exception {
        final String sql = person.update(person.id.set(person.id.add(1).map(Mappers.LONG))).show(new GenericDialect());
        assertSimilar("UPDATE person SET id = person.id + ?", sql);
    }

    public void testMultipleColumns() throws Exception {
        final String sql = person.update(person.parentId.set(person.id), person.name.set("John")).show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = person.id, name = ?", sql);
    }

    public void testWhere() throws Exception {
        final AbstractUpdateStatement updateStatement = person.update(person.parentId.set(person.id), person.name.set("John")).where(person.id.eq(1L));
        final String sql = updateStatement.show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = person.id, name = ? WHERE person.id = ?", sql);
        final String sql2 = updateStatement.show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testSubqueryInWhere() throws Exception {
        final Person child = new Person();
        final String sql = person.update(person.parentId.set(person.id)).where(child.id.where(child.parentId.eq(person.id)).exists()).show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = person.id WHERE EXISTS(SELECT T0.id FROM person AS T0 WHERE T0.parent_id = person.id)", sql);
    }

    public void testSubqueryAsSource() throws Exception {
        final Person child = new Person();
        final String sql = person.update(person.name.set(child.name.where(child.parentId.eq(person.id)).queryValue())).show(new GenericDialect());
        assertSimilar("UPDATE person SET name =(SELECT T0.name FROM person AS T0 WHERE T0.parent_id = person.id)", sql);
    }

    public void testSubqueryFromNoTables() throws Exception {
        try {
            final String sql = person.update(person.id.set(person.parentId.where(person.id.eq(1L)).queryValue())).show(new GenericDialect());
            fail("MalformedStatementException expected but was " + sql);
        } catch (MalformedStatementException e) {
            assertEquals(e.getMessage(), "At least one table is required for FROM clause");
        }
        try {
            final String sql = person.update(person.id.set(person.parentId.where(person.id.eq(1L)).queryValue())).show(new GenericDialect(), Option.allowNoTables(true));
            fail("MalformedStatementException expected but was " + sql);
        } catch (MalformedStatementException e) {
            assertEquals(e.getMessage(), "Generic dialect does not support selects with no tables");
        }
    }

    public void testWrongTarget() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.update(child.name.set(person.name)).show(new GenericDialect());
            fail("MalformedStatementException expected, but was " + sql);
        } catch (MalformedStatementException e) {
            // fine
            assertTrue(e.getMessage().contains("is not legal in this context"));
        }
    }

    public void testWrongSource() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.update(person.name.set(child.name)).show(new GenericDialect());
            fail("MalformedStatementException expected, but was " + sql);
        } catch (MalformedStatementException e) {
            // fine
            assertTrue(e.getMessage().contains("is not legal in this context"));
        }
    }

//    public void testExecute() throws Exception {
//        new ExecuteScenario() {
//            @Override
//            protected void runExecute(final AbstractUpdateStatementBase update, final DatabaseGate gate) throws SQLException {
//                assertEquals(2, update.execute(gate));
//            }
//        }.play();
//
//    }

    private abstract static class ExecuteScenario {
        public void play() throws Exception {
            final AbstractUpdateStatementBase update = person.update(person.name.set("John"));
            final String statementString = update.show(new GenericDialect());
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(new GenericDialect());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(statementString)).andReturn(statement);
            statement.setString(1, "John");
            expect(statement.executeUpdate()).andReturn(2);
            statement.close();
            connection.close();
            replay(gate, connection,  statement);

            runExecute(update, gate);

            verify(gate, connection, statement);
        }

        protected abstract void runExecute(final AbstractUpdateStatementBase update, final DatabaseGate gate) throws SQLException;
    }

//    public void testExecuteSearched() throws Exception {
//        new ExecuteSearchedScenario() {
//            @Override
//            protected void runExecute(final AbstractUpdateStatement update, final DatabaseGate gate) throws SQLException {
//                assertEquals(1, update.execute(gate));
//            }
//        }.play();
//
//    }

    private abstract static class ExecuteSearchedScenario {
        public void play() throws Exception {
            final AbstractUpdateStatement update = person.update(person.name.set("John")).where(person.id.eq(1L));
            final String statementString = update.show(new GenericDialect());
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
            expect(gate.getDialect()).andReturn(new GenericDialect());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(statementString)).andReturn(statement);
            statement.setString(1, "John");
            statement.setLong(2, 1L);
            expect(statement.executeUpdate()).andReturn(1);
            statement.close();
            connection.close();
            replay(gate, connection,  statement);

            runExecute(update, gate);

            verify(gate, connection, statement);
        }

        protected abstract void runExecute(final AbstractUpdateStatement update, final DatabaseGate gate) throws SQLException;
    }

//    public void testExecuteWithOptions() throws Exception {
//        new ExecuteWithOptionsScenario() {
//            @Override
//            protected void runExecute(final AbstractUpdateStatement update, final DatabaseGate gate) throws SQLException {
//                assertEquals(1, update.execute(gate, Option.setQueryTimeout(20)));
//            }
//        }.play();
//
//    }

    private abstract static class ExecuteWithOptionsScenario {
        public void play() throws Exception {
            final AbstractUpdateStatement update = person.update(person.name.set("John")).where(person.id.eq(1L));
            final String statementString = update.show(new GenericDialect());
            final DatabaseGate gate = createMock(DatabaseGate.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            expect(gate.getOptions()).andReturn(Collections.<Option>singletonList(Option.setQueryTimeout(10)));
            expect(gate.getDialect()).andReturn(new GenericDialect());
            expect(gate.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(statementString)).andReturn(statement);
            statement.setQueryTimeout(10);
            statement.setQueryTimeout(20);
            statement.setString(1, "John");
            statement.setLong(2, 1L);
            expect(statement.executeUpdate()).andReturn(1);
            statement.close();
            connection.close();
            replay(gate, connection,  statement);

            runExecute(update, gate);

            verify(gate, connection, statement);
        }

        protected abstract void runExecute(final AbstractUpdateStatement update, final DatabaseGate gate) throws SQLException;
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
