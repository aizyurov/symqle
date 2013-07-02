package org.symqle.integration;

import org.symqle.common.Mappers;
import org.symqle.common.Pair;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.One;
import org.symqle.integration.model.UpdateTable;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class UpdateTest extends AbstractIntegrationTestBase {

    public void testUpdate() throws Exception {
        final UpdateTable updateTable = clean();
        final int affectedRows = updateTable
                .insert(updateTable.id.set(2), updateTable.text.set("wow"))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);

        updateTable.update(updateTable.id.set(3), updateTable.text.set("changed")).execute(getDatabaseGate());
        final List<Pair<Integer,String>> newRows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(3, "changed")), newRows);
    }

    private UpdateTable clean() throws SQLException {
        final UpdateTable updateTable = new UpdateTable();
        updateTable.delete().execute(getDatabaseGate());
        return updateTable;
    }

    public void testPartialSetList() throws Exception {
        final UpdateTable updateTable = clean();
        final int affectedRows = updateTable
                .insert(updateTable.id.set(2))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, "nothing")), rows);

        updateTable.update(updateTable.text.set("changed")).execute(getDatabaseGate());
        final List<Pair<Integer,String>> newRows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, "changed")), newRows);
    }

    public void testSetNull() throws Exception {
        final UpdateTable updateTable = clean();
        final int affectedRows = updateTable
                .insert(updateTable.id.set(2), updateTable.text.set("wow"))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);

        updateTable.update(updateTable.text.setNull()).execute(getDatabaseGate());
        final List<Pair<Integer,String>> newRows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, (String) null)), newRows);

    }

    public void testSetDefault() throws Exception {
        final UpdateTable updateTable = clean();
        final int affectedRows = updateTable
                .insert(updateTable.id.set(2), updateTable.text.set("wow"))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);

        updateTable.update(updateTable.text.setDefault()).execute(getDatabaseGate());
        final List<Pair<Integer,String>> newRows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, "nothing")), newRows);
    }

    public void testSetIgnoreType() throws Exception {
        final UpdateTable updateTable = clean();
        final int affectedRows = updateTable
                .insert(updateTable.id.set(2), updateTable.text.set("wow"))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);

        updateTable.update(updateTable.id.set(updateTable.id.add(1).map(Mappers.INTEGER))).execute(getDatabaseGate());
        final List<Pair<Integer,String>> newRows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(3, "wow")), newRows);
    }

    public void testSetSubquery() throws Exception {
        final UpdateTable updateTable = clean();
        final int affectedRows = updateTable
                .insert(updateTable.id.set(2), updateTable.text.set("wow"))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);

        final One one = new One();
        final Employee employee = new Employee();
        final int updatedRowsCount = updateTable.update(
                updateTable.id.set(one.id.queryValue()),
                updateTable.text.set(employee.firstName.where(employee.lastName.eq("Redwood")).queryValue()))
                .execute(getDatabaseGate());
        assertEquals(1, updatedRowsCount);
        final List<Pair<Integer,String>> updatedRows = updateTable.id.pair(updateTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(1, "Margaret")), updatedRows);
    }

}
