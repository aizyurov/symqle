package org.symqle.integration;

import org.symqle.Mappers;
import org.symqle.Pair;
import org.symqle.front.Params;
import org.symqle.integration.model.Employee;
import org.symqle.integration.model.InsertTable;
import org.symqle.integration.model.One;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class InsertTest extends AbstractIntegrationTestBase {

    public void testInsert() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2), insertTable.text.set("wow"))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);
    }

    private InsertTable clean() throws SQLException {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getDatabaseGate());
        return insertTable;
    }

    public void testDefaults() throws Exception {
        final InsertTable insertTable = clean();
        try {
            final int affectedRows = insertTable.insertDefault().execute(getDatabaseGate());
            assertEquals(1, affectedRows);
            final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDatabaseGate());
            assertEquals(Arrays.asList(Pair.make(1, "nothing")), rows);
        } catch (SQLException e) {
            // mysql: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'DEFAULT VALUES'
            // derby: ERROR 42X01: Syntax error: Encountered "DEFAULT" at line 1, column 25
            expectSQLException(e, "Apache Derby", "MySQL");
        }
    }

    public void testPartialSetList() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, "nothing")), rows);
    }

    public void testSetNull() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2), insertTable.text.setNull())
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(2, (String) null)), rows);
    }

    public void testSetDefault() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(3), insertTable.text.setDefault())
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(3, "nothing")), rows);
    }

    public void testSetIgnoreType() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(Params.p(3)), insertTable.text.set("three"))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(3, "three")), rows);
    }

    public void testSetSubquery() throws Exception {
        final InsertTable insertTable = clean();
        final One one = new One();
        final Employee employee = new Employee();
        final int affectedRows = insertTable.insert(
                insertTable.id.set(one.id.queryValue()),
                insertTable.text.set(employee.firstName.where(employee.lastName.eq("Redwood")).queryValue()))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(1, "Margaret")), rows);
    }

    public void testSetExpression() throws Exception {
        final InsertTable insertTable = clean();
        final One one = new One();
        final Employee employee = new Employee();
        final int affectedRows = insertTable.insert(
                insertTable.id.set(one.id.queryValue().add(2).map(Mappers.INTEGER)),
                insertTable.text.set(employee.firstName.where(employee.lastName.eq("Redwood")).queryValue()))
                .execute(getDatabaseGate());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDatabaseGate());
        assertEquals(Arrays.asList(Pair.make(3, "Margaret")), rows);

    }

}
