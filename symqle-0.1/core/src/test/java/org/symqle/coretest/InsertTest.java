package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractInsertStatement;
import org.symqle.sql.Column;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Collections;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class InsertTest extends SqlTestCase {

    public void testOneColumn() throws Exception {
        final AbstractInsertStatement insert = person.insert(person.parentId.set(DynamicParameter.create(Mappers.LONG, 1L)));
        final String sql = insert.show();
        assertSimilar("INSERT INTO person(parent_id) VALUES(?)", sql);
        assertSimilar(sql, insert.show(GenericDialect.get()));
    }

    public void testSetNull() throws Exception {
        final String sql = person.insert(person.parentId.setNull()).show();
        assertSimilar("INSERT INTO person(parent_id) VALUES(NULL)", sql);
    }

    public void testSetDefault() throws Exception {
        final String sql = person.insert(person.parentId.setDefault()).show();
        assertSimilar("INSERT INTO person(parent_id) VALUES(DEFAULT)", sql);
    }

    public void testInsertDefault() throws Exception {
        final String sql = person.insertDefault().show();
        assertSimilar("INSERT INTO person DEFAULT VALUES", sql);
    }

    public void testSetOverrideType() throws Exception {
        final String sql = person.insert(person.id.set(DynamicParameter.create(Mappers.STRING, "1").map(Mappers.LONG))).show();
        assertSimilar("INSERT INTO person(id) VALUES(?)", sql);
    }

    public void testMultipleColumns() throws Exception {
        final String sql = person.insert(person.parentId.set(DynamicParameter.create(Mappers.LONG, 1L)), person.name.set("John Doe")).show();
        assertSimilar("INSERT INTO person(parent_id, name) VALUES(?, ?)", sql);
    }

    public void testSubqueryAsSource() throws Exception {
        final Person child = new Person();
        final String sql = person.insert(person.name.set(child.name.where(child.id.eq(1L)).queryValue())).show();
        assertSimilar("INSERT INTO person(name) VALUES((SELECT T0.name FROM person AS T0 WHERE T0.id = ?))", sql);
    }

    public void testSubqueryFromNoTables() throws Exception {
        try {
            final String sql = person.insert(person.id.set(person.parentId.where(person.id.eq(1L)).queryValue())).show();
            fail("IllegalStateException expected but was " + sql);
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "At least one table is required for FROM clause");
        }
    }

    public void testWrongTarget() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.insert(child.name.set(person.name)).show();
            fail("IllegalArgumentException expected, but was " + sql);
        } catch (IllegalArgumentException e) {
            // fine
            assertTrue(e.getMessage().contains("is not legal in this context"));
        }
    }

    public void testWrongSource() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.insert(person.name.set(child.name)).show();
            fail("IllegalArgumentException expected, but was " + sql);
        } catch (IllegalArgumentException e) {
            // fine
            assertTrue(e.getMessage().contains("is not legal in this context"));
        }
    }

    public void testExecute() throws Exception {
        final AbstractInsertStatement update = person.insert(person.name.set("John"));
        final String statementString = update.show();
        final DatabaseGate gate = createMock(DatabaseGate.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        expect(gate.getOptions()).andReturn(Collections.<Option>emptyList());
        expect(gate.getDialect()).andReturn(GenericDialect.get());
        expect(gate.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(statementString)).andReturn(statement);
        statement.setString(1, "John");
        expect(statement.executeUpdate()).andReturn(2);
        statement.close();
        connection.close();
        replay(gate, connection, statement);

        assertEquals(2, update.execute(gate));

        verify(gate, connection,  statement);

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