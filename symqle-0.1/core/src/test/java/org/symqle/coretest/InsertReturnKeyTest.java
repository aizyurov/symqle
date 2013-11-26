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
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Symqle;
import org.symqle.sql.Table;

import java.util.Arrays;

import static org.easymock.EasyMock.createMock;
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
                    new MockEngine(1, Arrays.asList(123L), statementString, parameters, new SqlContext.Builder().toSqlContext()));
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


}
