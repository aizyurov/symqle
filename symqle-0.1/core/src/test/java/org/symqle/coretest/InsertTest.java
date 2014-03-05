package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.MalformedStatementException;
import org.symqle.common.OutBox;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.sql.*;

import java.util.Arrays;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class InsertTest extends SqlTestCase {

    public void testOneColumn() throws Exception {
        final AbstractInsertStatement insert = person.insert(person.parentId.set(DynamicParameter.create(CoreMappers.LONG, 1L)));
        final String sql = insert.show(new GenericDialect());
        assertSimilar("INSERT INTO person(parent_id) VALUES(?)", sql);
        assertSimilar(sql, insert.show(new GenericDialect()));
    }

    public void testAdaptSetClause() throws Exception {
        final SetClause setClause = person.parentId.set(DynamicParameter.create(CoreMappers.LONG, 1L));
        final AbstractInsertStatement insert = person.insert(AbstractSetClause.adapt(setClause));
        final String sql = insert.show(new GenericDialect());
        assertSimilar("INSERT INTO person(parent_id) VALUES(?)", sql);
        assertSimilar(sql, insert.show(new GenericDialect()));
    }

    public void testAdapt() throws Exception {
        final AbstractInsertStatement insert = person.insert(person.parentId.set(DynamicParameter.create(CoreMappers.LONG, 1L)));
        final AbstractInsertStatement adaptor = AbstractInsertStatement.adapt(insert);
        assertEquals(insert.show(new GenericDialect()), adaptor.show(new GenericDialect()));
    }

    public void testSetNull() throws Exception {
        final String sql = person.insert(person.parentId.setNull()).show(new GenericDialect());
        assertSimilar("INSERT INTO person(parent_id) VALUES(NULL)", sql);
    }

    public void testSetDefault() throws Exception {
        final String sql = person.insert(person.parentId.setDefault()).show(new GenericDialect());
        assertSimilar("INSERT INTO person(parent_id) VALUES(DEFAULT)", sql);
    }

    public void testInsertDefault() throws Exception {
        final String sql = person.insertDefault().show(new GenericDialect());
        assertSimilar("INSERT INTO person DEFAULT VALUES", sql);
    }

    public void testSetOverrideType() throws Exception {
        final String sql = person.insert(person.id.set(DynamicParameter.create(CoreMappers.STRING, "1").map(CoreMappers.LONG))).show(new GenericDialect());
        assertSimilar("INSERT INTO person(id) VALUES(?)", sql);
    }

    public void testMultipleColumns() throws Exception {
        final String sql = person.insert(person.parentId.set(DynamicParameter.create(CoreMappers.LONG, 1L)).also(person.name.set("John Doe"))).show(new GenericDialect());
        assertSimilar("INSERT INTO person(parent_id, name) VALUES(?, ?)", sql);
    }

    public void testSubqueryAsSource() throws Exception {
        final Person child = new Person();
        final String sql = person.insert(person.name.set(child.name.where(child.id.eq(1L)).queryValue())).show(new GenericDialect());
        assertSimilar("INSERT INTO person(name) VALUES((SELECT T0.name FROM person AS T0 WHERE T0.id = ?))", sql);
    }

    public void testSystemConstant() throws Exception {
        final String sql = person.insert(person.id.set(Symqle.currentTimestamp().map(CoreMappers.LONG))).show(new GenericDialect());
        assertEquals("INSERT INTO person(id) VALUES(CURRENT_TIMESTAMP)", sql);
    }

    public void testSourceIsTarget() throws Exception {
        try {
            final String sql = person.insert(person.id.set(person.parentId)).show(new GenericDialect());
            fail("MalformedStatementException expected but was " + sql);
        } catch (MalformedStatementException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Illegal in this context"));
        }
    }

    public void testWrongTarget() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.insert(child.name.set(person.name)).show(new GenericDialect());
            fail("MalformedStatementException expected, but was " + sql);
        } catch (MalformedStatementException e) {
            // fine
            assertTrue(e.getMessage().contains("Illegal in this context"));
        }
    }

    public void testWrongSource() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.insert(person.name.set(child.name)).show(new GenericDialect());
            fail("MalformedStatementException expected, but was " + sql);
        } catch (MalformedStatementException e) {
            // fine
            assertTrue(e.getMessage().contains("Illegal in this context"));
        }
    }

    public void testExecute() throws Exception {
        final AbstractInsertStatement update = person.insert(person.name.set("John"));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        expect(parameters.next()).andReturn(param);
        param.setString("John");
        replay(parameters, param);
        int rows = update.execute(
                new MockEngine(3, statementString, parameters, new SqlContext.Builder().toSqlContext()));
        assertEquals(3, rows);
        verify(parameters, param);
    }

    public void testCompile() throws Exception {
        final AbstractInsertStatement update = person.insert(person.name.set("John"));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        expect(parameters.next()).andReturn(param);
        param.setString("John");
        replay(parameters, param);
        int rows = update.compileUpdate(
                new MockEngine(3, statementString, parameters, new SqlContext.Builder().toSqlContext())).execute();
        assertEquals(3, rows);
        verify(parameters, param);
    }

    public void testSubmit() throws Exception {
        final AbstractInsertStatement update = person.insert(person.name.set("John"));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        expect(parameters.next()).andReturn(param);
        param.setString("John");
        replay(parameters, param);
        int[] rows = update.submit(
                new MockEngine(1, statementString, parameters, new SqlContext.Builder().toSqlContext()).newBatcher(1));
        assertTrue(Arrays.toString(rows), Arrays.equals(new int[] {1}, rows));
        verify(parameters, param);
    }

    public void testExecuteWithNoTables() throws Exception {
        final AbstractInsertStatement update = person.insert(person.name.set(Symqle.currentDate().map(CoreMappers.STRING)));
        final String statementString = update.show(new OracleLikeDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        replay(parameters, param);
        int rows = update.execute(
                new MockEngine(3, statementString, parameters, SqlContextUtil.allowNoTablesContext()));
        assertEquals(3, rows);
        verify(parameters, param);
    }

    private static class Person extends Table {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<Long> age = defineColumn(CoreMappers.LONG, "age");
        public Column<Long> parentId = defineColumn(CoreMappers.LONG, "parent_id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

    private static Person person = new Person();


}