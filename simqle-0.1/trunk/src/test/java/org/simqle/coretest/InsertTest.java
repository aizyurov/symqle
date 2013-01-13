package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class InsertTest extends SqlTestCase {

    public void testOneColumn() throws Exception {
        final String sql = person.insert(person.parentId.set(DynamicParameter.create(Mappers.LONG, 1L))).show();
        assertSimilar("INSERT INTO person(parent_id) VALUES(?)", sql);
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
        final String sql = person.update(person.id.setIgnoreType(person.id.plus(1))).show();
        assertSimilar("UPDATE person SET id = person.id + ?", sql);
    }

    public void testMultipleColumns() throws Exception {
        final String sql = person.update(person.parentId.set(person.id), person.name.set("John")).show();
        assertSimilar("UPDATE person SET parent_id = person.id, name = ?", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.update(person.parentId.set(person.id), person.name.set("John")).where(person.id.eq(1L)).show();
        assertSimilar("UPDATE person SET parent_id = person.id, name = ? WHERE person.id = ?", sql);
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

        assertEquals(2, update.execute(datasource));

    }

    public void testExecuteSearched() throws Exception {
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