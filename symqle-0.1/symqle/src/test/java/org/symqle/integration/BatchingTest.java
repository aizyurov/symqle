package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.InsertTable;
import org.symqle.jdbc.Batcher;
import org.symqle.jdbc.Engine;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractUpdateStatement;

import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class BatchingTest extends AbstractIntegrationTestBase {

    public void testInsert() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final Engine engine = getEngine();
        insertTable.delete().execute(engine);
        final Batcher batcher = engine.newBatcher(5);
        for (int i =0; i<10; i++) {
            final int[] affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(batcher);
            if (i == 5) {
                assertTrue(Arrays.toString(affectedRows), Arrays.equals(new int[] {1,1,1,1,1}, affectedRows));
            } else {
                assertEquals(0, affectedRows.length);
            }
        }
        assertEquals(5, batcher.flush().length);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<10; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("a"+i, rows.get(i).second());
        }
    }

    public void testNegativeBatchSize() throws Exception {
        try {
            getEngine().newBatcher(-10);
            fail("Negative batch size accepted");
        } catch (IllegalArgumentException e) {
            // fine
        }
    }

    public void testDifferentStatement() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final Engine engine = getEngine();
        insertTable.delete().execute(engine);
        final Batcher batcher = engine.newBatcher(10);
        for (int i =0; i<5; i++) {
            final int[] affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(batcher);
            assertEquals(0, affectedRows.length);
        }
        int[] affectedRows = insertTable.insert(insertTable.id.set(5)).submit(batcher);
        assertTrue(Arrays.toString(affectedRows), Arrays.equals(new int[]{1, 1, 1, 1, 1}, affectedRows));
        final int[] flush = batcher.flush();
        assertEquals(1, flush.length);
        assertEquals(1, flush[0]);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<5; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("a"+i, rows.get(i).second());
        }
        assertEquals(5, rows.get(5).first().intValue());
        assertEquals("nothing", rows.get(5).second());
    }

    public void testDifferentOptions() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final Engine engine = getEngine();
        insertTable.delete().execute(engine);
        final Batcher batcher = engine.newBatcher(10);
        for (int i =0; i<5; i++) {
            final int[] affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(batcher);
            assertEquals(0, affectedRows.length);
        }
        final int[] affectedRows = insertTable.insert(insertTable.id.set(5), insertTable.text.set("a" + 5)).submit(batcher, Option.setFetchSize(10));
        assertTrue(Arrays.toString(affectedRows), Arrays.equals(new int[] {1,1,1,1,1}, affectedRows));
        final int[] flush = batcher.flush();
        assertEquals(1, flush.length);
        assertEquals(1, flush[0]);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<5; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("a"+i, rows.get(i).second());
        }
        assertEquals(5, rows.get(5).first().intValue());
        assertEquals("a5", rows.get(5).second());
    }

    public void testUpdate() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final Engine engine = getEngine();
        insertTable.delete().execute(engine);
        final Batcher batcher = engine.newBatcher(5);
        for (int i =0; i<10; i++) {
            final int[] affectedRows = insertTable.insert(insertTable.id.set(i)).submit(batcher);
            if (i == 5) {
                assertEquals(5, affectedRows.length);
            } else {
                assertEquals(0, affectedRows.length);
            }
        }
        assertEquals(5, batcher.flush().length);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<10; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("nothing", rows.get(i).second());
        }

        for (int i =0; i<10; i++) {
            final AbstractUpdateStatement updateStatement = insertTable.update(insertTable.text.set("a" + i)).where(insertTable.id.eq(i));
            final int[] affectedRows = updateStatement.submit(batcher);
            if (i == 5) {
                assertEquals(5, affectedRows.length);
            } else {
                assertEquals(0, affectedRows.length);
            }
        }
        assertEquals(5, batcher.flush().length);
        final List<Pair<Integer,String>> updated = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<10; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("a"+i, updated.get(i).second());
        }
    }


}
