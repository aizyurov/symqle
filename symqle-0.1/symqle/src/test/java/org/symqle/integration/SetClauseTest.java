package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.InsertTable;
import org.symqle.testset.AbstractSetClauseTestSet;

import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class SetClauseTest extends AbstractIntegrationTestBase implements AbstractSetClauseTestSet {

    @Override
    public void test_also_SetClauseList() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .compileUpdate(getEngine())
                .execute();
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);
    }

    @Override
    public void test_also_SetClauseList_SetClauseList_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .compileUpdate(getEngine())
                .execute();
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);
    }

    @Override
    public void test_insert_TargetTable_SetClauseList_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2))
                .compileUpdate(getEngine())
                .execute();
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "nothing")), rows);
    }

    @Override
    public void test_update_TargetTable_SetClauseList_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2))
                .compileUpdate(getEngine())
                .execute();
        assertEquals(1, affectedRows);
        insertTable.update(insertTable.text.set("wow")).where(insertTable.id.eq(2)).execute(getEngine());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);
    }
}
