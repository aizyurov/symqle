package org.symqle.coretest;

import org.symqle.common.MalformedStatementException;
import org.symqle.common.Mappers;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameter;
import org.symqle.common.SqlParameters;
import org.symqle.sql.AbstractDeleteStatement;
import org.symqle.sql.AbstractDeleteStatementBase;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.Table;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class DeleteTest extends SqlTestCase {

    public void testDeleteAll() throws Exception {
        final AbstractDeleteStatementBase deleteStatementBase = person.delete();
        final String sql = deleteStatementBase.show(new GenericDialect());
        assertSimilar("DELETE FROM person", sql);
        final String sql2 = deleteStatementBase.show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testWhere() throws Exception {
        final AbstractDeleteStatement deleteStatement = person.delete().where(person.id.eq(1L));
        final String sql = deleteStatement.show(new GenericDialect());
        assertSimilar("DELETE FROM person WHERE person.id = ?", sql);
        assertSimilar(sql, deleteStatement.show(new GenericDialect()));
    }

    public void testSubqueryInWhere() throws Exception {
        final Person child = new Person();
        final String sql = person.delete().where(child.id.where(child.parentId.eq(person.id)).exists()).show(new GenericDialect());
        assertSimilar("DELETE FROM person WHERE EXISTS(SELECT T0.id FROM person AS T0 WHERE T0.parent_id = person.id)", sql);
    }

    public void testWrongCondition() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.delete().where(child.name.eq("John")).show(new GenericDialect());
            fail("MalformedStatementException expected, but was " + sql);
        } catch (MalformedStatementException e) {
            // fine
            assertTrue(e.getMessage().contains("Not legal in this context"));
        }
    }

    public void testExecute() throws Exception {
        final AbstractDeleteStatementBase update = person.delete();
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        replay(parameters);
        final int affectedRows = update.execute(
                new MockEngine(2, null, statementString, parameters, new SqlContext()));
        assertEquals(2, affectedRows);
        verify(parameters);

    }

    public void testExecuteSearched() throws Exception {
        final AbstractDeleteStatement update = person.delete().where(person.id.eq(1L));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final SqlParameter param = createMock(SqlParameter.class);
        expect(parameters.next()).andReturn(param);
        param.setLong(1L);
        replay(parameters, param);
        final int affectedRows = update.execute(
                new MockEngine(2, null, statementString, parameters, new SqlContext()));
        assertEquals(2, affectedRows);
        verify(parameters, param);
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
