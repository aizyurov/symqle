package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.InsertTable;
import org.symqle.jdbc.Batcher;
import org.symqle.sql.AbstractUpdateStatement;
import org.symqle.testset.AbstractUpdateStatementTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class UpdateStatementTest extends AbstractIntegrationTestBase implements AbstractUpdateStatementTestSet {


    @Override
    public void test_compileUpdate_Engine_Option() throws Exception {
        final InsertTable insertTable = clean();
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one")))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two")))
                .execute(getEngine());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text)
                .orderBy(insertTable.id)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "one"), Pair.make(2, "two")), rows);

        final AbstractUpdateStatement updateStatement = insertTable.update(insertTable.text.set("changed"))
                .where(insertTable.id.eq(1));
        updateStatement.compileUpdate(getEngine()).execute();
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text)
                .orderBy(insertTable.id)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "changed"), Pair.make(2, "two")), newRows);
    }

    @Override
    public void test_execute_Engine_Option() throws Exception {
        final InsertTable insertTable = clean();
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one")))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two")))
                .execute(getEngine());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text)
                .orderBy(insertTable.id)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "one"), Pair.make(2, "two")), rows);

        final AbstractUpdateStatement updateStatement = insertTable.update(insertTable.text.set("changed"))
                .where(insertTable.id.eq(1));
        updateStatement.execute(getEngine());
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text)
                .orderBy(insertTable.id)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "changed"), Pair.make(2, "two")), newRows);
    }

    @Override
    public void test_showUpdate_Dialect_Option() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final AbstractUpdateStatement updateStatement = insertTable.update(insertTable.text.set("changed"))
                .where(insertTable.id.eq(1));
        final String sql = updateStatement.showUpdate(getEngine().getDialect());
        assertEquals("UPDATE insert_test SET text = ? WHERE insert_test.id = ?", sql);
    }

    @Override
    public void test_submit_Batcher_Option() throws Exception {
        final InsertTable insertTable = clean();
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("one")))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.set("two")))
                .execute(getEngine());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text)
                .orderBy(insertTable.id)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "one"), Pair.make(2, "two")), rows);
        final AbstractUpdateStatement updateStatement = insertTable.update(insertTable.text.set("changed"))
                .where(insertTable.id.eq(1));
        final Batcher batcher = getEngine().newBatcher(10);
        final int[] affected = updateStatement.submit(batcher);
        assertEquals(0, affected.length);
        final int[] flushed = batcher.flush();
        assertTrue(Arrays.toString(flushed), Arrays.equals(new int[]{1}, flushed));
        final List<Pair<Integer,String>> newRows = insertTable.id.pair(insertTable.text)
                .orderBy(insertTable.id)
                .list(getEngine());
        assertEquals(Arrays.asList(Pair.make(1, "changed"), Pair.make(2, "two")), newRows);
    }

    private InsertTable clean() throws SQLException {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        return insertTable;
    }


}
