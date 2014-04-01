package org.symqle.integration;

import org.symqle.common.CoreMappers;
import org.symqle.common.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.GeneratedKeysTable;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.One;
import org.symqle.jdbc.Batcher;
import org.symqle.jdbc.GeneratedKeys;
import org.symqle.sql.AbstractInsertStatement;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.InsertStatement;
import org.symqle.sql.Params;
import org.symqle.testset.AbstractInsertStatementTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class InsertTest extends AbstractIntegrationTestBase implements AbstractInsertStatementTestSet {

    @Override
    public void test_adapt_InsertStatement() throws Exception {
        final InsertTable insertTable = clean();
        final InsertStatement insertStatement = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")));
        final int affectedRows = AbstractInsertStatement.adapt(insertStatement)
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);
    }

    @Override
    public void test_compileUpdate_Engine_Option() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .compileUpdate(getEngine())
                .execute();
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);
    }

    @Override
    public void test_compileUpdate_GeneratedKeys_Engine_Option() throws Exception {
        final GeneratedKeysTable generatedKeysTable = new GeneratedKeysTable();
        generatedKeysTable.delete().execute(getEngine());
        final GeneratedKeys<Integer> generatedKeys = GeneratedKeys.create(generatedKeysTable.id().getMapper());
        assertEquals(1, generatedKeysTable.insert(generatedKeysTable.text().set("Bim")).compileUpdate(generatedKeys, getEngine()).execute());
        assertEquals(1, generatedKeysTable.insert(generatedKeysTable.text().set("Bom")).compileUpdate(generatedKeys, getEngine()).execute());
        final List<Integer> allKeys = generatedKeys.all();
        assertEquals(2, allKeys.size());
        assertTrue(allKeys.get(1) > allKeys.get(0));
        final List<String> bimList = generatedKeysTable.text().where(generatedKeysTable.id().eq(allKeys.get(0))).list(getEngine());
        assertEquals(Arrays.asList("Bim"), bimList);
    }

    @Override
    public void test_execute_Engine_Option() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);
    }

    @Override
    public void test_execute_GeneratedKeys_Engine_Option() throws Exception {
        final GeneratedKeysTable generatedKeysTable = new GeneratedKeysTable();
        generatedKeysTable.delete().execute(getEngine());
        final GeneratedKeys<Integer> generatedKeys = GeneratedKeys.create(generatedKeysTable.id().getMapper());
        assertEquals(1, generatedKeysTable.insert(generatedKeysTable.text().set("Bim")).execute(generatedKeys, getEngine()));
        assertEquals(1, generatedKeysTable.insert(generatedKeysTable.text().set("Bom")).execute(generatedKeys, getEngine()));
        final List<Integer> allKeys = generatedKeys.all();
        assertEquals(2, allKeys.size());
        assertTrue(allKeys.get(1) > allKeys.get(0));
        final List<String> bimList = generatedKeysTable.text().where(generatedKeysTable.id().eq(allKeys.get(0))).list(getEngine());
        assertEquals(Arrays.asList("Bim"), bimList);
    }

    @Override
    public void test_showUpdate_Dialect_Option() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final String sql = insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .showUpdate(getEngine().getDialect());
        assertEquals("INSERT INTO insert_test(id, text) VALUES(?, ?)", sql);

    }

    @Override
    public void test_submit_Batcher_Option() throws Exception {
        final InsertTable insertTable = clean();
        final DynamicParameter<Integer> idParam = insertTable.id.param();
        final DynamicParameter<String> textParam = insertTable.text.param();
        final AbstractInsertStatement insert = insertTable
                .insert(insertTable.id.set(idParam).also(insertTable.text.set(textParam)));
        final Batcher batcher = getEngine().newBatcher(10);
        idParam.setValue(1);
        textParam.setValue("one");
        assertEquals(0, insert.submit(batcher).length);
        idParam.setValue(2);
        textParam.setValue("two");
        assertEquals(0, insert.submit(batcher).length);
        final int[] flushed = batcher.flush();
        assertTrue(Arrays.toString(flushed), Arrays.equals(new int[] {1, 1}, flushed));

        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text)
                .orderBy(insertTable.id)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "one"), Pair.make(2, "tow")), rows);
    }

    public void testInsert() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);
    }

    private InsertTable clean() throws SQLException {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        return insertTable;
    }

    public void testDefaults() throws Exception {
        final InsertTable insertTable = clean();
        try {
            final int affectedRows = insertTable.insertDefault().execute(getEngine());
            assertEquals(1, affectedRows);
            final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
            assertEquals(Arrays.asList(Pair.make(0, "nothing")), rows);
        } catch (SQLException e) {
            // mysql: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'DEFAULT VALUES'
            // derby: ERROR 42X01: Syntax error: Encountered "DEFAULT" at line 1, column 25
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    public void testPartialSetList() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "nothing")), rows);
    }

    public void testSetNull() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.setNull()))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, (String) null)), rows);
    }

    public void testSetDefault() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(3).also(insertTable.text.setDefault()))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(3, "nothing")), rows);
    }

    public void testSetIgnoreType() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(Params.p(3)).also(insertTable.text.set("three")))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(3, "three")), rows);
    }

    public void testSetSubquery() throws Exception {
        final InsertTable insertTable = clean();
        final One one = new One();
        final Employee employee = new Employee();
        final int affectedRows = insertTable.insert(
                insertTable.id.set(one.id.queryValue()).also(
                insertTable.text.set(employee.firstName.where(employee.lastName.eq("Redwood")).queryValue())))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "Margaret")), rows);
    }

    public void testSetExpression() throws Exception {
        final InsertTable insertTable = clean();
        final One one = new One();
        final Employee employee = new Employee();
        final int affectedRows = insertTable.insert(
                insertTable.id.set(one.id.queryValue().add(2).map(CoreMappers.INTEGER)).also(
                insertTable.text.set(employee.firstName.where(employee.lastName.eq("Redwood")).queryValue())))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(3, "Margaret")), rows);

    }

}
