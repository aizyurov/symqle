package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.AbstractUpdateStatement;
import org.simqle.sql.AbstractUpdateStatementBase;
import org.simqle.sql.Column;
import org.simqle.sql.DialectDataSource;
import org.simqle.sql.GenericDialect;
import org.simqle.sql.Table;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class UpdateTest extends SqlTestCase {

    public void testOneColumn() throws Exception {
        final String sql = person.update(person.parentId.set(person.id)).show();
        assertSimilar("UPDATE person SET parent_id = person.id", sql);
        final String sql2 = person.update(person.parentId.set(person.id)).show(GenericDialect.get());
        assertSimilar(sql, sql2);
    }

    public void testSetNull() throws Exception {
        final String sql = person.update(person.parentId.setNull()).show();
        assertSimilar("UPDATE person SET parent_id = NULL", sql);
    }

    public void testSetDefault() throws Exception {
        final String sql = person.update(person.parentId.setDefault()).show();
        assertSimilar("UPDATE person SET parent_id = DEFAULT", sql);
    }

    public void testSetIgnoreType() throws Exception {
        final String sql = person.update(person.id.setIgnoreType(person.id.add(1))).show();
        assertSimilar("UPDATE person SET id = person.id + ?", sql);
    }

    public void testMultipleColumns() throws Exception {
        final String sql = person.update(person.parentId.set(person.id), person.name.set("John")).show();
        assertSimilar("UPDATE person SET parent_id = person.id, name = ?", sql);
    }

    public void testWhere() throws Exception {
        final AbstractUpdateStatement updateStatement = person.update(person.parentId.set(person.id), person.name.set("John")).where(person.id.eq(1L));
        final String sql = updateStatement.show();
        assertSimilar("UPDATE person SET parent_id = person.id, name = ? WHERE person.id = ?", sql);
        final String sql2 = updateStatement.show(GenericDialect.get());
        assertSimilar(sql, sql2);
    }

    public void testSubqueryInWhere() throws Exception {
        final Person child = new Person();
        final String sql = person.update(person.parentId.set(person.id)).where(child.id.where(child.parentId.eq(person.id)).exists()).show();
        assertSimilar("UPDATE person SET parent_id = person.id WHERE EXISTS(SELECT T0.id FROM person AS T0 WHERE T0.parent_id = person.id)", sql);
    }

    public void testSubqueryAsSource() throws Exception {
        final Person child = new Person();
        final String sql = person.update(person.name.set(child.name.where(child.parentId.eq(person.id)).queryValue())).show();
        assertSimilar("UPDATE person SET name =(SELECT T0.name FROM person AS T0 WHERE T0.parent_id = person.id)", sql);
    }

    public void testSubqueryFromNoTables() throws Exception {
        try {
            final String sql = person.update(person.id.set(person.parentId.where(person.id.eq(1L)).queryValue())).show();
            fail("IllegalStateException expected but was " + sql);
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("does not support"));
        }
    }

    public void testWrongTarget() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.update(child.name.set(person.name)).show();
            fail("IllegalArgumentException expected, but was " + sql);
        } catch (IllegalArgumentException e) {
            // fine
            assertTrue(e.getMessage().contains("is not legal in this context"));
        }
    }

    public void testWrongSource() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.update(person.name.set(child.name)).show();
            fail("IllegalArgumentException expected, but was " + sql);
        } catch (IllegalArgumentException e) {
            // fine
            assertTrue(e.getMessage().contains("is not legal in this context"));
        }
    }

    public void testExecute() throws Exception {
        new ExecuteScenario() {
            @Override
            protected void runExecute(final AbstractUpdateStatementBase update, final DataSource datasource) throws SQLException {
                assertEquals(2, update.execute(datasource));
            }
        }.play();

        new ExecuteScenario() {
            @Override
            protected void runExecute(final AbstractUpdateStatementBase update, final DataSource datasource) throws SQLException {
                assertEquals(2, update.execute(new DialectDataSource(GenericDialect.get(), datasource)));
            }
        }.play();

    }

    private abstract static class ExecuteScenario {
        public void play() throws Exception {
            final AbstractUpdateStatementBase update = person.update(person.name.set("John"));
            final String statementString = update.show();
            final DataSource datasource = createMock(DataSource.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            expect(datasource.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(statementString)).andReturn(statement);
            statement.setString(1, "John");
            expect(statement.executeUpdate()).andReturn(2);
            statement.close();
            connection.close();
            replay(datasource, connection,  statement);

            runExecute(update, datasource);

            verify(datasource, connection, statement);
        }

        protected abstract void runExecute(final AbstractUpdateStatementBase update, final DataSource datasource) throws SQLException;
    }

    public void testExecuteSearched() throws Exception {
        new ExecuteSearchedScenario() {
            @Override
            protected void runExecute(final AbstractUpdateStatement update, final DataSource datasource) throws SQLException {
                assertEquals(1, update.execute(datasource));
            }
        }.play();

        new ExecuteSearchedScenario() {
            @Override
            protected void runExecute(final AbstractUpdateStatement update, final DataSource datasource) throws SQLException {
                assertEquals(1, update.execute(new DialectDataSource(GenericDialect.get(), datasource)));
            }
        }.play();

    }

    private abstract static class ExecuteSearchedScenario {
        public void play() throws Exception {
            final AbstractUpdateStatement update = person.update(person.name.set("John")).where(person.id.eq(1L));
            final String statementString = update.show();
            final DataSource datasource = createMock(DataSource.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            expect(datasource.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(statementString)).andReturn(statement);
            statement.setString(1, "John");
            statement.setLong(2, 1L);
            expect(statement.executeUpdate()).andReturn(1);
            statement.close();
            connection.close();
            replay(datasource, connection,  statement);

            runExecute(update, datasource);

            verify(datasource, connection, statement);
        }

        protected abstract void runExecute(final AbstractUpdateStatement update, final DataSource datasource) throws SQLException;
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
