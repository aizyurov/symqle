package org.simqle.integration;

import org.simqle.Pair;
import org.simqle.front.Params;
import org.simqle.integration.model.Employee;
import org.simqle.integration.model.InsertTable;
import org.simqle.integration.model.One;

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
                .execute(getDialectDataSource());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make(2, "wow")), rows);
    }

    private InsertTable clean() throws SQLException {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getDialectDataSource());
        return insertTable;
    }

    public void testDefaults() throws Exception {
        final InsertTable insertTable = clean();
        try {
            final int affectedRows = insertTable.insertDefault().execute(getDialectDataSource());
            assertEquals(1, affectedRows);
            final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDialectDataSource());
            assertEquals(Arrays.asList(Pair.make(1, "nothing")), rows);
        } catch (SQLException e) {
            // mysql: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'DEFAULT VALUES'
            // derby: ERROR 42X01: Syntax error: Encountered "DEFAULT" at line 1, column 25
            expectSQLException(e, "derby", "mysql");
        }
    }

    public void testPartialSetList() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2))
                .execute(getDialectDataSource());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make(2, "nothing")), rows);
    }

    public void testSetNull() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(2), insertTable.text.setNull())
                .execute(getDialectDataSource());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make(2, (String) null)), rows);
    }

    public void testSetDefault() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.set(3), insertTable.text.setDefault())
                .execute(getDialectDataSource());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make(3, "nothing")), rows);
    }

    public void testSetIgnoreType() throws Exception {
        final InsertTable insertTable = clean();
        final int affectedRows = insertTable
                .insert(insertTable.id.setIgnoreType(Params.p(3)), insertTable.text.set("three"))
                .execute(getDialectDataSource());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make(3, "three")), rows);
    }

    public void testSetSubquery() throws Exception {
        final InsertTable insertTable = clean();
        final One one = new One();
        final Employee employee = new Employee();
        final int affectedRows = insertTable.insert(
                insertTable.id.set(one.id.queryValue()),
                insertTable.text.set(employee.firstName.where(employee.lastName.eq("Redwood")).queryValue()))
                .execute(getDialectDataSource());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make(1, "Margaret")), rows);
    }

    public void testSetExpression() throws Exception {
        final InsertTable insertTable = clean();
        final One one = new One();
        final Employee employee = new Employee();
        final int affectedRows = insertTable.insert(
                insertTable.id.setIgnoreType(one.id.queryValue().plus(2)),
                insertTable.text.set(employee.firstName.where(employee.lastName.eq("Redwood")).queryValue()))
                .execute(getDialectDataSource());
        assertEquals(1, affectedRows);
        final List<Pair<Integer,String>> rows = insertTable.id.pair(insertTable.text).list(getDialectDataSource());
        assertEquals(Arrays.asList(Pair.make(3, "Margaret")), rows);

    }

}
