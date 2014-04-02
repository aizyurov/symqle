package org.symqle.integration;

import org.symqle.integration.model.DeleteMaster;
import org.symqle.jdbc.Batcher;
import org.symqle.testset.AbstractDeleteStatementTestSet;

import java.util.Arrays;

/**
 * @author lvovich
 */
public class DeleteStatementTest extends AbstractIntegrationTestBase implements AbstractDeleteStatementTestSet {

    @Override
    public void test_compileUpdate_Engine_Option() throws Exception {
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
    public void test_execute_Engine_Option() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        master.delete().execute(getEngine());
        master.insert(master.masterId.set(1).also(master.description.set("one"))).execute(getEngine());
        master.insert(master.masterId.set(2).also(master.description.set("two"))).execute(getEngine());
        master.insert(master.masterId.set(3).also(master.description.set("three"))).execute(getEngine());
        assertEquals(Arrays.asList(1, 2, 3), master.masterId.list(getEngine()));
        assertEquals(2, master.delete().where(master.masterId.lt(3)).execute(getEngine()));
        assertEquals(Arrays.asList(3), master.masterId.list(getEngine()));
    }

    @Override
    public void test_showUpdate_Dialect_Option() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        final String sql = master.delete().where(master.masterId.lt(3)).showUpdate(getEngine().getDialect());
        if (getDatabaseName().equals("PostgreSQL")) {
            assertEquals("DELETE FROM delete_master WHERE(delete_master.master_id < ?)", sql);
        } else {
            assertEquals("DELETE FROM delete_master WHERE delete_master.master_id < ?", sql);
        }
    }

    @Override
    public void test_submit_Batcher_Option() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        master.delete().execute(getEngine());
        master.insert(master.masterId.set(1).also(master.description.set("one"))).execute(getEngine());
        master.insert(master.masterId.set(2).also(master.description.set("two"))).execute(getEngine());
        master.insert(master.masterId.set(3).also(master.description.set("three"))).execute(getEngine());
        assertEquals(Arrays.asList(1, 2, 3), master.masterId.list(getEngine()));
        final Batcher batcher = getEngine().newBatcher(10);
        final int[] submitted = master.delete().where(master.masterId.lt(3)).submit(batcher);
        assertEquals(0, submitted.length);
        final int[] flushed = batcher.flush();
        assertTrue(Arrays.toString(flushed), Arrays.equals(new int[]{2}, flushed));
        assertEquals(Arrays.asList(3), master.masterId.list(getEngine()));
    }
}
