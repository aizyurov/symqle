package org.symqle.integration;

import org.symqle.common.CompiledSql;
import org.symqle.common.SqlParameters;
import org.symqle.integration.model.DeleteDetail;
import org.symqle.integration.model.DeleteMaster;
import org.symqle.jdbc.Batcher;
import org.symqle.jdbc.Option;
import org.symqle.querybuilder.StringSql;
import org.symqle.sql.AbstractDeleteStatementBase;
import org.symqle.testset.AbstractDeleteStatementBaseTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class DeleteStatementBaseTest extends AbstractIntegrationTestBase implements AbstractDeleteStatementBaseTestSet {

    private final static List<Option> NO_OPTIONS = Collections.<Option>emptyList();

    @Override
    protected void onSetUp() throws Exception {
        getEngine().execute(new CompiledSql(new StringSql("DELETE FROM delete_detail")), NO_OPTIONS);
        getEngine().execute(new CompiledSql(new StringSql("DELETE FROM delete_master")), NO_OPTIONS);
    }

    private CompiledSql createInsertIntoDeleteMaster(final int id, final String description) {
        return new CompiledSql(new StringSql("INSERT INTO delete_master (master_id, description) values (?, ?)") {

            @Override
            public void setParameters(SqlParameters p) throws SQLException {
                p.next().setInt(id);
                p.next().setString(description);
            }
        });
    }

    private CompiledSql createInsertIntoDeleteDetail(final int id, final int masterId, final String description) {
        return new CompiledSql(new StringSql("INSERT INTO delete_detail (detail_id, master_id, detail) values (?, ?, ?)") {

            @Override
            public void setParameters(SqlParameters p) throws SQLException {
                p.next().setInt(id);
                p.next().setInt(masterId);
                p.next().setString(description);
            }
        });
    }

    @Override
    public void test_compileUpdate_Engine_Option() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        final AbstractDeleteStatementBase delete = master.delete();
        master.delete().execute(getEngine());
        getEngine().execute(createInsertIntoDeleteMaster(1, "one"), NO_OPTIONS);
        getEngine().execute(createInsertIntoDeleteMaster(2, "two"), NO_OPTIONS);
        assertEquals(Arrays.asList(1, 2), master.masterId.list(getEngine()));
        assertEquals(2, delete.compileUpdate(getEngine()).execute());
        assertEquals(Collections.<Integer>emptyList(), master.masterId.list(getEngine()));
    }

    @Override
    public void test_execute_Engine_Option() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        final AbstractDeleteStatementBase delete = master.delete();
        master.delete().execute(getEngine());
        getEngine().execute(createInsertIntoDeleteMaster(1, "one"), NO_OPTIONS);
        getEngine().execute(createInsertIntoDeleteMaster(2, "two"), NO_OPTIONS);
        assertEquals(Arrays.asList(1, 2), master.masterId.list(getEngine()));
        assertEquals(2, delete.execute(getEngine()));
        assertEquals(Collections.<Integer>emptyList(), master.masterId.list(getEngine()));
    }

    @Override
    public void test_showUpdate_Dialect_Option() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        final AbstractDeleteStatementBase delete = master.delete();
        assertEquals("DELETE FROM delete_master", delete.showUpdate(getEngine().getDialect()));
    }

    @Override
    public void test_submit_Batcher_Option() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        final AbstractDeleteStatementBase delete = master.delete();
        master.delete().execute(getEngine());
        getEngine().execute(createInsertIntoDeleteMaster(1, "one"), NO_OPTIONS);
        getEngine().execute(createInsertIntoDeleteMaster(2, "two"), NO_OPTIONS);
        final Batcher batcher = getEngine().newBatcher(10);
        final int[] submitted = delete.submit(batcher);
        assertEquals(0, submitted.length);
        assertEquals(Arrays.asList(1, 2), master.masterId.list(getEngine()));
        final int[] flushed = batcher.flush();
        assertTrue(Arrays.toString(flushed), Arrays.equals(new int[]{2}, flushed));
    }

    @Override
    public void test_where_WhereClause() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        master.delete().execute(getEngine());
        getEngine().execute(createInsertIntoDeleteMaster(1, "one"), NO_OPTIONS);
        getEngine().execute(createInsertIntoDeleteMaster(2, "two"), NO_OPTIONS);
        getEngine().execute(createInsertIntoDeleteMaster(3, "three"), NO_OPTIONS);
        assertEquals(Arrays.asList(1, 2, 3), master.masterId.list(getEngine()));
        assertEquals(2, master.delete().where(master.masterId.lt(3)).execute(getEngine()));
        assertEquals(Arrays.asList(3), master.masterId.list(getEngine()));
    }

    public void testDeleteBySubquery() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        master.delete().execute(getEngine());

        getEngine().execute(createInsertIntoDeleteMaster(1, "one"), NO_OPTIONS);
        getEngine().execute(createInsertIntoDeleteMaster(2, "two"), NO_OPTIONS);
        getEngine().execute(createInsertIntoDeleteDetail(1, 1, "Detail 1/1"), NO_OPTIONS);
        getEngine().execute(createInsertIntoDeleteDetail(2, 1, "Detail 2/1"), NO_OPTIONS);
        final DeleteDetail detail = new DeleteDetail();
        assertEquals(Arrays.asList(1, 2), master.masterId.list(getEngine()));
        try {
            // delete all master records, which HAVE detail records
            assertEquals(1, master.delete()
                    .where(detail.detailId.where(detail.masterId.eq(master.masterId)).exists())
                    .execute(getEngine()));
            fail("Constrain violation expected");
        } catch (SQLException e) {
            // fine
        }
            // delete all master records, which have no detail records
        assertEquals(1, master.delete()
                .where(detail.detailId.where(detail.masterId.eq(master.masterId)).exists().negate())
                .execute(getEngine()));
        assertEquals(Arrays.asList(1), master.masterId.list(getEngine()));
    }
}
