package org.symqle.coretest;

import junit.framework.TestCase;
import org.symqle.common.MalformedStatementException;
import org.symqle.common.Mappers;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameter;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractInsertStatement;
import org.symqle.sql.Column;
import org.symqle.sql.DatabaseGate;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Symqle;
import org.symqle.sql.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class InsertReturnKeyTest extends TestCase {

    public void testExecute() throws Exception {
        final AbstractInsertStatement update = person.insert(person.name.set(Symqle.currentDate().map(Mappers.STRING)));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final SqlParameter param =createMock(SqlParameter.class);
        replay(parameters, param);
        Long key = update.executeReturnKey(person.id,
                new MockEngine(1, 123L, statementString, parameters, SqlContextUtil.allowNoTablesContext(), Option.allowNoTables(false)), Option.allowNoTables(false));
        assertEquals(123L, key.longValue());
        verify(parameters, param);
    }

    public void testWrongColumn() throws Exception {
        final AbstractInsertStatement update = person.insert(person.name.set("John"));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        replay(parameters);
        try {
            Long key = update.executeReturnKey(another.id,
                    new MockEngine(1, Arrays.asList(123L), statementString, parameters, new SqlContext()));
            assertEquals(123L, key.longValue());
            fail("MalformedStatementException expected");
        } catch (MalformedStatementException e) {
            // expected
        }
        verify(parameters);
    }

    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();

    private static Person another = new Person();

    public void testExecuteWithOptions() throws Exception {
        final AbstractInsertStatement update = person.insert(person.name.set("John"));
        final String statementString = update.show(new GenericDialect());
        final DatabaseGate gate = createMock(DatabaseGate.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet generatedKeys = createMock(ResultSet.class);
        expect(gate.getOptions()).andReturn(Collections.<Option>singletonList(Option.setQueryTimeout(30)));
        expect(gate.getDialect()).andReturn(new GenericDialect());
        expect(gate.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(statementString, Statement.RETURN_GENERATED_KEYS)).andReturn(statement);
        statement.setQueryTimeout(30);
        statement.setFetchSize(10);
        statement.setString(1, "John");
        expect(statement.executeUpdate()).andReturn(1);
        expect(statement.getGeneratedKeys()).andReturn(generatedKeys);
        expect(generatedKeys.next()).andReturn(true);
        expect(generatedKeys.getLong(1)).andReturn(123L);
        expect(generatedKeys.wasNull()).andReturn(false);
        generatedKeys.close();
        statement.close();
        connection.close();
        replay(gate, connection, statement, generatedKeys);

//        assertEquals(123L, update.executeReturnKey(person.id, gate, Option.setFetchSize(10)).longValue());

//        verify(gate, connection,  statement, generatedKeys);
    }


}
