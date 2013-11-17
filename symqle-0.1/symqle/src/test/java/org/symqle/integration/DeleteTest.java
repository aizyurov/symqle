package org.symqle.integration;

import org.symqle.integration.model.DeleteDetail;
import org.symqle.integration.model.DeleteMaster;
import org.symqle.jdbc.Engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author lvovich
 */
public class DeleteTest extends AbstractIntegrationTestBase {

    @Override
    protected void onSetUp() throws Exception {
        getEngine().execute(new Engine.ConnectionCallback() {
            @Override
            public void call(final Connection connection) throws SQLException {
            {
                final PreparedStatement stmt = connection.prepareStatement("DELETE FROM delete_detail");
                stmt.executeUpdate();
                stmt.close();
            }
            {
                final PreparedStatement stmt = connection.prepareStatement("DELETE FROM delete_master");
                stmt.executeUpdate();
                stmt.close();
            }
            }
        });
    }

    public void testDeleteAll() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        assertEquals(0, master.delete().execute(getEngine()));
        getEngine().execute(new Engine.ConnectionCallback() {
            @Override
            public void call(final Connection connection) throws SQLException {
                final PreparedStatement stmt = connection.prepareStatement("INSERT INTO delete_master (master_id, description) values (?, ?)");
                try {
                    stmt.setInt(1, 1);
                    stmt.setString(2, "one");
                    stmt.executeUpdate();
                    stmt.setInt(1, 2);
                    stmt.setString(2, "two");
                    stmt.executeUpdate();
                    stmt.close();
                } finally {
                    stmt.close();
                }
            }
        });
        assertEquals(Arrays.asList(1, 2), master.masterId.list(getEngine()));
        assertEquals(2, master.delete().execute(getEngine()));
        assertEquals(Collections.<Integer>emptyList(), master.masterId.list(getEngine()));
    }

    public void testDeleteSome() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        assertEquals(0, master.delete().execute(getEngine()));
        getEngine().execute(new Engine.ConnectionCallback() {
            @Override
            public void call(final Connection connection) throws SQLException {
                final PreparedStatement stmt = connection.prepareStatement("INSERT INTO delete_master (master_id, description) values (?, ?)");
                try {
                    stmt.setInt(1, 1);
                    stmt.setString(2, "one");
                    stmt.executeUpdate();
                    stmt.setInt(1, 2);
                    stmt.setString(2, "two");
                    stmt.executeUpdate();
                    stmt.setInt(1, 3);
                    stmt.setString(2, "three");
                    stmt.executeUpdate();
                    stmt.close();
                } finally {
                    stmt.close();
                }
            }
        });
        assertEquals(Arrays.asList(1, 2, 3), master.masterId.list(getEngine()));
        assertEquals(2, master.delete().where(master.masterId.lt(3)).execute(getEngine()));
        assertEquals(Arrays.asList(3), master.masterId.list(getEngine()));
    }

    public void testDeleteBySubquery() throws Exception {
        final DeleteMaster master = new DeleteMaster();
        assertEquals(0, master.delete().execute(getEngine()));
        getEngine().execute(new Engine.ConnectionCallback() {
            @Override
            public void call(final Connection connection) throws SQLException {
                {
                    final PreparedStatement stmt = connection.prepareStatement("INSERT INTO delete_master (master_id, description) values (?, ?)");
                    stmt.setInt(1, 1);
                    stmt.setString(2, "one");
                    stmt.executeUpdate();
                    stmt.setInt(1, 2);
                    stmt.setString(2, "two");
                    stmt.executeUpdate();
                    stmt.close();
                }
                {
                    final PreparedStatement stmt = connection.prepareStatement("INSERT INTO delete_detail (detail_id, master_id, detail) values (?, ?, ?)");
                    stmt.setInt(1, 1);
                    stmt.setInt(2, 1);
                    stmt.setString(3, "detail 1/1");
                    stmt.executeUpdate();
                    stmt.setInt(1, 2);
                    stmt.setInt(2, 1);
                    stmt.setString(3, "detail 2/1");
                    stmt.executeUpdate();
                    stmt.close();
                }
            }
        });
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
