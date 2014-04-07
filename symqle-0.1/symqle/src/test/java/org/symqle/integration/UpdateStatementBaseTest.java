package org.symqle.integration;

import org.symqle.common.CoreMappers;
import org.symqle.common.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.One;
import org.symqle.integration.model.InsertTable;
import org.symqle.jdbc.Batcher;
import org.symqle.sql.AbstractUpdateStatementBase;
import org.symqle.testset.AbstractUpdateStatementBaseTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class UpdateStatementBaseTest extends AbstractIntegrationTestBase implements AbstractUpdateStatementBaseTestSet {


    @Override
    public void test_compileUpdate_Engine_Option() throws Exception {
        final InsertTable insertTable = clean();
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one")))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two")))
                .execute(getEngine());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "one"), Pair.make(2, "two")), rows);

        final AbstractUpdateStatementBase updateStatementBase = insertTable.update(insertTable.text.set("changed"));
        updateStatementBase.compileUpdate(getEngine()).execute();
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "changed"), Pair.make(2, "changed")), newRows);
    }

    @Override
    public void test_execute_Engine_Option() throws Exception {
        final InsertTable insertTable = clean();
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one")))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two")))
                .execute(getEngine());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "one"), Pair.make(2, "two")), rows);

        final AbstractUpdateStatementBase updateStatementBase = insertTable.update(insertTable.text.set("changed"));
        updateStatementBase.execute(getEngine());
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "changed"), Pair.make(2, "changed")), newRows);
    }

    @Override
    public void test_showUpdate_Dialect_Option() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final AbstractUpdateStatementBase updateStatementBase = insertTable.update(insertTable.text.set("changed"));
        final String sql = updateStatementBase.showUpdate(getEngine().getDialect());
        assertEquals("UPDATE insert_test SET text = ?", sql);
    }

    @Override
    public void test_submit_Batcher_Option() throws Exception {
        final InsertTable insertTable = clean();
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one")))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two")))
                .execute(getEngine());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "one"), Pair.make(2, "two")), rows);
        final AbstractUpdateStatementBase updateStatementBase = insertTable.update(insertTable.text.set("changed"));
        final Batcher batcher = getEngine().newBatcher(10);
        final int[] affected = updateStatementBase.submit(batcher);
        assertEquals(0, affected.length);
        final int[] flushed = batcher.flush();
        assertTrue(Arrays.toString(flushed), Arrays.equals(new int[]{2}, flushed));
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "changed"), Pair.make(2, "changed")), newRows);
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final InsertTable insertTable = clean();
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one")))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two")))
                .execute(getEngine());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "one"), Pair.make(2, "two")), rows);

        final AbstractUpdateStatementBase updateStatementBase = insertTable.update(insertTable.text.set("changed"));
        updateStatementBase.where(insertTable.id.eq(1)).execute(getEngine());
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "changed"), Pair.make(2, "two")), newRows);
    }

    private InsertTable clean() throws SQLException {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        return insertTable;
    }

    public void testPartialSetList() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "nothing")), rows);

        insertTable.update(insertTable.text.set("changed")).execute(getEngine());
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "changed")), newRows);
    }

    public void testSetNull() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);

        insertTable.update(insertTable.text.setNull()).execute(getEngine());
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, (String) null)), newRows);

    }

    public void testSetDefault() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);

        insertTable.update(insertTable.text.setDefault()).execute(getEngine());
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "nothing")), newRows);
    }

    public void testSetMapped() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);

        insertTable.update(insertTable.id.set(insertTable.id.add(1).map(CoreMappers.INTEGER))).execute(getEngine());
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(3, "wow")), newRows);
    }

    public void testSetSubquery() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);

        final One one = new One();
        final Employee employee = new Employee();
        final int updatedRowsCount = insertTable.update(
                insertTable.id.set(one.id.queryValue()).also(
                insertTable.text.set(employee.firstName.where(employee.lastName.eq("Redwood")).queryValue())))
                .execute(getEngine());
        assertEquals(1, updatedRowsCount);
        final List<Pair<Integer,String>> updatedRows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "Margaret")), updatedRows);
    }

}
