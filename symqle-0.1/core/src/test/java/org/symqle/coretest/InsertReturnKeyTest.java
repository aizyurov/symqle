package org.symqle.coretest;

import junit.framework.TestCase;
import org.symqle.common.CoreMappers;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.GeneratedKeys;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractInsertStatement;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Table;

import java.util.Collections;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class InsertReturnKeyTest extends TestCase {

    public void testExecute() throws Exception {
        final Person people = new Person();
        final AbstractInsertStatement update = person.insert(person.subordinatesCount.set(people.id.count().queryValue()));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        replay(parameters, param);
        final GeneratedKeys<Long> generatedKeys = GeneratedKeys.create(CoreMappers.LONG);
        int affectedRows = update.execute(generatedKeys, new MockEngine(1, statementString, parameters, SqlContextUtil.allowNoTablesContext(), Collections.<Option>singletonList(Option.allowNoTables(false))), Option.allowNoTables(false));
        assertEquals(1, affectedRows);
        verify(parameters, param);
    }

    public void testCompileExecute() throws Exception {
        final Person people = new Person();
        final AbstractInsertStatement update = person.insert(person.subordinatesCount.set(people.id.count().queryValue()));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        replay(parameters, param);
        final GeneratedKeys<Long> generatedKeys = GeneratedKeys.create(CoreMappers.LONG);
        int affectedRows = update.compileUpdate(generatedKeys, new MockEngine(1, statementString, parameters, SqlContextUtil.allowNoTablesContext(), Collections.<Option>singletonList(Option.allowNoTables(false))), Option.allowNoTables(false)).execute();
        assertEquals(1, affectedRows);
        verify(parameters, param);
    }

    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<Integer> subordinatesCount = defineColumn(CoreMappers.INTEGER, "total");
    }

    private static Person person = new Person();

    private static Person another = new Person();


}
