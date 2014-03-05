package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.common.MalformedStatementException;
import org.symqle.common.OutBox;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractSetClause;
import org.symqle.sql.AbstractUpdateStatement;
import org.symqle.sql.AbstractUpdateStatementBase;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.SetClause;
import org.symqle.sql.Table;

import java.util.Arrays;
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
        final AbstractUpdateStatementBase update = person.update(person.parentId.set(person.id));
        final String sql = update.show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = person.id", sql);
        final String sql2 = person.update(person.parentId.set(person.id)).show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testAdaptSetClause() throws Exception {
        final SetClause setClause = person.parentId.set(person.id);
        final AbstractUpdateStatementBase update = person.update(AbstractSetClause.adapt(setClause));
        final String sql = update.show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = person.id", sql);
        final String sql2 = person.update(person.parentId.set(person.id)).show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testAdapt() throws Exception {
        final AbstractUpdateStatementBase update = person.update(person.parentId.set(person.id));
        final AbstractUpdateStatement adaptor = AbstractUpdateStatement.adapt(update);
        final String sql = adaptor.show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = person.id", sql);
    }

    public void testAdaptBase() throws Exception {
        final AbstractUpdateStatementBase update = person.update(person.parentId.set(person.id));
        final AbstractUpdateStatementBase adaptor = AbstractUpdateStatementBase.adapt(update);
        final String sql = adaptor.show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = person.id", sql);

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
        final String sql = person.update(person.id.set(person.id.add(1).map(CoreMappers.LONG))).show(new GenericDialect());
        assertSimilar("UPDATE person SET id = person.id + ?", sql);
    }

    public void testMultipleColumns() throws Exception {
        final String sql = person.update(person.parentId.set(person.id).also(person.name.set("John")).
        also(person.age.set(28L))).show(new GenericDialect());
        assertSimilar("UPDATE person SET parent_id = person.id, name = ?, age = ?", sql);
    }

    public void testWhere() throws Exception {
        final AbstractUpdateStatement updateStatement = person.update(person.parentId.set(person.id).also(person.name.set("John"))).where(person.id.eq(1L));
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
            e.printStackTrace();
            assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("Illegal in this context"));
        }
    }

    public void testWrongSource() throws Exception {
        final Person child = new Person();
        try {
            final String sql = person.update(person.name.set(child.name)).show(new GenericDialect());
            fail("MalformedStatementException expected, but was " + sql);
        } catch (MalformedStatementException e) {
            // fine
            assertTrue(e.getMessage().contains("Illegal in this context"));
        }
    }

    public void testExecute() throws Exception {
        final AbstractUpdateStatementBase update = person.update(person.name.set("John"));
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
        final AbstractUpdateStatementBase update = person.update(person.name.set("John"));
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
        final AbstractUpdateStatementBase update = person.update(person.name.set("John"));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        expect(parameters.next()).andReturn(param);
        param.setString("John");
        replay(parameters, param);
        int[] rows = update.submit(
                new MockEngine(3, statementString, parameters, new SqlContext.Builder().toSqlContext()).newBatcher(1));
        assertTrue(Arrays.toString(rows), Arrays.equals(new int[] {1,1,1}, rows));
        verify(parameters, param);
    }

    public void testExecuteWithOtherDialect() throws Exception {
        final AbstractUpdateStatementBase update = person.update(person.name.set("John"));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        expect(parameters.next()).andReturn(param);
        param.setString("John");
        replay(parameters, param);
        int rows = update.execute(
                new MockEngine(3, statementString, parameters, SqlContextUtil.allowNoTablesContext()));
        assertEquals(3, rows);
        verify(parameters, param);
    }

    public void testExecuteSearched() throws Exception {
        final AbstractUpdateStatement update = person.update(person.name.set("John")).where(person.id.eq(1L));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        final OutBox param2 =createMock(OutBox.class);
        expect(parameters.next()).andReturn(param);
        param.setString("John");
        expect(parameters.next()).andReturn(param2);
        param2.setLong(1L);
        replay(parameters, param, param2);
        int rows = update.execute(
                new MockEngine(3, statementString, parameters, new SqlContext.Builder().toSqlContext()));
        assertEquals(3, rows);
        verify(parameters, param, param2);
    }

    public void testCompileSearched() throws Exception {
        final AbstractUpdateStatement update = person.update(person.name.set("John")).where(person.id.eq(1L));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        final OutBox param2 =createMock(OutBox.class);
        expect(parameters.next()).andReturn(param);
        param.setString("John");
        expect(parameters.next()).andReturn(param2);
        param2.setLong(1L);
        replay(parameters, param, param2);
        int rows = update.compileUpdate(
                new MockEngine(3, statementString, parameters, new SqlContext.Builder().toSqlContext())).execute();
        assertEquals(3, rows);
        verify(parameters, param, param2);
    }

    public void testSubmitSearched() throws Exception {
        final AbstractUpdateStatement update = person.update(person.name.set("John")).where(person.id.eq(1L));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        final OutBox param2 =createMock(OutBox.class);
        expect(parameters.next()).andReturn(param);
        param.setString("John");
        expect(parameters.next()).andReturn(param2);
        param2.setLong(1L);
        replay(parameters, param, param2);
        int[] rows = update.submit(
                new MockEngine(3, statementString, parameters, new SqlContext.Builder().toSqlContext()).newBatcher(1));
        assertTrue(Arrays.toString(rows), Arrays.equals(new int[] {1,1,1}, rows));
        verify(parameters, param, param2);
    }

    public void testExecuteWithOptions() throws Exception {
        final AbstractUpdateStatement update = person.update(person.name.set("John")).where(person.id.eq(1L));
        final String statementString = update.show(new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final OutBox param =createMock(OutBox.class);
        final OutBox param2 =createMock(OutBox.class);
        expect(parameters.next()).andReturn(param);
        param.setString("John");
        expect(parameters.next()).andReturn(param2);
        param2.setLong(1L);
        replay(parameters, param, param2);
        int rows = update.execute(
                new MockEngine(3, statementString, parameters, new SqlContext.Builder().toSqlContext(), Collections.<Option>singletonList(Option.setQueryTimeout(20))),
                Option.setQueryTimeout(20));
        assertEquals(3, rows);
        verify(parameters, param, param2);
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
