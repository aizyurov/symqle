package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.DeleteDetail;
import org.symqle.integration.model.DeleteMaster;
import org.symqle.integration.model.InsertTable;
import org.symqle.sql.AbstractUpdateStatement;
import org.symqle.testset.TableTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class TableTest extends AbstractIntegrationTestBase implements TableTestSet {

    @Override
    public void test_delete_() throws Exception {
        final DeleteDetail deleteDetail = new DeleteDetail();
        deleteDetail.delete().execute(getEngine());
        final DeleteMaster master = new DeleteMaster();
        master.delete().execute(getEngine());
        master.insert(master.masterId.set(1).also(master.description.set("one"))).execute(getEngine());
        master.insert(master.masterId.set(2).also(master.description.set("two"))).execute(getEngine());
        master.insert(master.masterId.set(3).also(master.description.set("three"))).execute(getEngine());
        assertEquals(Arrays.asList(1, 2, 3), master.masterId.list(getEngine()));
        assertEquals(2, master.delete().where(master.masterId.lt(3)).compileUpdate(getEngine()).execute());
        assertEquals(Arrays.asList(3), master.masterId.list(getEngine()));
    }

    @Override
    public void test_insertDefault_() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        try {
            final int affectedRows = insertTable.insertDefault().execute(getEngine());
            assertEquals(1, affectedRows);
            final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
            assertEquals(Arrays.asList(Pair.make(0, "nothing")), rows);
        } catch (SQLException e) {
            // mysql: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'DEFAULT VALUES'
            // derby: ERROR 42X01: Syntax error: Encountered "DEFAULT" at line 1, column 25
            expectSQLException(e, SupportedDb.APACHE_DERBY, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_insert_SetClauseList() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2).also(insertTable.text.set("wow")))
                .execute(getEngine());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getEngine());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);
    }

    @Override
    public void test_update_SetClauseList() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
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
}
