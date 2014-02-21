package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.InsertTable;
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
        engine.setBatchSize(5);
        assertEquals(5, engine.getBatchSize());
        for (int i =0; i<10; i++) {
            final int[] affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(engine);
            if (i == 5) {
                assertTrue(Arrays.toString(affectedRows), Arrays.equals(new int[] {1,1,1,1,1}, affectedRows));
            } else {
                assertEquals(0, affectedRows.length);
            }
        }
        assertEquals(5, engine.flush().length);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<10; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("a"+i, rows.get(i).second());
        }
    }

    public void testNegativeBatchSize() throws Exception {
        try {
            getEngine().setBatchSize(-10);
            fail("Negative batch size accepted");
        } catch (IllegalArgumentException e) {
            // fine
        }
    }

    public void testDecreaseBatchSize() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final Engine engine = getEngine();
        insertTable.delete().execute(engine);
        engine.setBatchSize(5);
        for (int i =0; i<5; i++) {
            final int[] affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(engine);
            assertEquals(0, affectedRows.length);
        }
        final int[] rows = engine.setBatchSize(3);
        assertTrue(Arrays.toString(rows), Arrays.equals(new int[]{1, 1, 1, 1, 1}, rows));
        assertEquals(0, engine.flush().length);
        final List<Pair<Integer,String>> list = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<5; i++) {
            assertEquals(i, list.get(i).first().intValue());
            assertEquals("a"+i, list.get(i).second());
        }
    }

    public void testIncreaseBatchSize() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final Engine engine = getEngine();
        insertTable.delete().execute(engine);
        engine.setBatchSize(5);
        for (int i =0; i<5; i++) {
            final int[] affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(engine);
            assertEquals(0, affectedRows.length);
        }
        assertEquals(0, engine.setBatchSize(10).length);
        final int[] rows = engine.flush();
        assertTrue(Arrays.toString(rows), Arrays.equals(new int[]{1, 1, 1, 1, 1}, rows));
        final List<Pair<Integer,String>> list = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<5; i++) {
            assertEquals(i, list.get(i).first().intValue());
            assertEquals("a" + i, list.get(i).second());
        }
    }

    public void testDifferentStatement() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final Engine engine = getEngine();
        insertTable.delete().execute(engine);
        engine.setBatchSize(10);
        for (int i =0; i<5; i++) {
            final int[] affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(engine);
            assertEquals(0, affectedRows.length);
        }
        int[] affectedRows = insertTable.insert(insertTable.id.set(5)).submit(engine);
        assertTrue(Arrays.toString(affectedRows), Arrays.equals(new int[]{1, 1, 1, 1, 1}, affectedRows));
        final int[] flush = engine.flush();
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
        engine.setBatchSize(10);
        for (int i =0; i<5; i++) {
            final int[] affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(engine);
            assertEquals(0, affectedRows.length);
        }
        final int[] affectedRows = insertTable.insert(insertTable.id.set(5), insertTable.text.set("a" + 5)).submit(engine, Option.setFetchSize(10));
        assertTrue(Arrays.toString(affectedRows), Arrays.equals(new int[] {1,1,1,1,1}, affectedRows));
        final int[] flush = engine.flush();
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
        engine.setBatchSize(5);
        for (int i =0; i<10; i++) {
            final int[] affectedRows = insertTable.insert(insertTable.id.set(i)).submit(engine);
            if (i == 5) {
                assertEquals(5, affectedRows.length);
            } else {
                assertEquals(0, affectedRows.length);
            }
        }
        assertEquals(5, engine.flush().length);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<10; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("nothing", rows.get(i).second());
        }

        for (int i =0; i<10; i++) {
            final AbstractUpdateStatement updateStatement = insertTable.update(insertTable.text.set("a" + i)).where(insertTable.id.eq(i));
            final int[] affectedRows = updateStatement.submit(engine);
            if (i == 5) {
                assertEquals(5, affectedRows.length);
            } else {
                assertEquals(0, affectedRows.length);
            }
        }
        assertEquals(5, engine.flush().length);
        final List<Pair<Integer,String>> updated = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<10; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("a"+i, updated.get(i).second());
        }
    }


}
