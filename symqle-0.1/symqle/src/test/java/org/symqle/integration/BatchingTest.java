package org.symqle.integration;

import org.symqle.common.Pair;
import org.symqle.integration.model.InsertTable;
import org.symqle.jdbc.Engine;
import org.symqle.jdbc.Option;
import org.symqle.sql.AbstractUpdateStatement;

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
            final int affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(engine);
            assertTrue("i= " + i +", affectedRows= " + affectedRows, i == 5 && affectedRows == 5 || i !=5 && affectedRows == Engine.NOTHING_FLUSHED);
        }
        assertEquals(5, engine.flush());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<10; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("a"+i, rows.get(i).second());
        }
    }

    public void testDecreaseBatchSize() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final Engine engine = getEngine();
        insertTable.delete().execute(engine);
        engine.setBatchSize(5);
        for (int i =0; i<5; i++) {
            final int affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(engine);
            assertTrue("i= " + i +", affectedRows= " + affectedRows, affectedRows == Engine.NOTHING_FLUSHED);
        }
        assertEquals(5, engine.setBatchSize(3));
        assertEquals(Engine.NOTHING_FLUSHED, engine.flush());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<5; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("a"+i, rows.get(i).second());
        }
    }

    public void testIncreaseBatchSize() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final Engine engine = getEngine();
        insertTable.delete().execute(engine);
        engine.setBatchSize(5);
        for (int i =0; i<5; i++) {
            final int affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(engine);
            assertTrue("i= " + i +", affectedRows= " + affectedRows, affectedRows == Engine.NOTHING_FLUSHED);
        }
        assertEquals(Engine.NOTHING_FLUSHED, engine.setBatchSize(10));
        assertEquals(5, engine.flush());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<5; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("a"+i, rows.get(i).second());
        }
    }

    public void testDifferentStatement() throws Exception {
        final InsertTable insertTable = new InsertTable();
        final Engine engine = getEngine();
        insertTable.delete().execute(engine);
        engine.setBatchSize(10);
        for (int i =0; i<5; i++) {
            final int affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(engine);
            assertTrue("i= " + i +", affectedRows= " + affectedRows, affectedRows == Engine.NOTHING_FLUSHED);
        }
        int affectedRows = insertTable.insert(insertTable.id.set(5)).submit(engine);
        assertEquals(5, affectedRows);
        assertEquals(1, engine.flush());
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
            final int affectedRows = insertTable.insert(insertTable.id.set(i), insertTable.text.set("a" + i)).submit(engine);
            assertTrue("i= " + i +", affectedRows= " + affectedRows, affectedRows == Engine.NOTHING_FLUSHED);
        }
        final int affectedRows = insertTable.insert(insertTable.id.set(5), insertTable.text.set("a" + 5)).submit(engine, Option.setQueryTimeout(1));
        assertEquals(5, affectedRows);
        assertEquals(1, engine.flush());
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
            final int affectedRows = insertTable.insert(insertTable.id.set(i)).submit(engine);
            assertTrue("i= " + i +", affectedRows= " + affectedRows, i == 5 && affectedRows == 5 || i !=5 && affectedRows == Engine.NOTHING_FLUSHED);
        }
        assertEquals(5, engine.flush());
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<10; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("nothing", rows.get(i).second());
        }

        for (int i =0; i<10; i++) {
            final AbstractUpdateStatement updateStatement = insertTable.update(insertTable.text.set("a" + i)).where(insertTable.id.eq(i));
            final int affectedRows = updateStatement.submit(engine);
            assertTrue("i= " + i +", affectedRows= " + affectedRows, i == 5 && affectedRows == 5 || i !=5 && affectedRows == Engine.NOTHING_FLUSHED);
        }
        assertEquals(5, engine.flush());
        final List<Pair<Integer,String>> updated = insertTable.id.pair(insertTable.text).orderBy(insertTable.id).list(engine);
        for (int i=0; i<10; i++) {
            assertEquals(i, rows.get(i).first().intValue());
            assertEquals("a"+i, updated.get(i).second());
        }
    }


}
