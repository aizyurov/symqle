package org.symqle.integration;

import org.symqle.integration.model.InsertTable;
import org.symqle.sql.AbstractSortOrderingSpecification;
import org.symqle.testset.AbstractSortOrderingSpecificationTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class SortOrderingSpecificationTest extends AbstractIntegrationTestBase implements AbstractSortOrderingSpecificationTestSet {

    @Override
    public void test_nullsFirst_() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("abc")))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull()))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("xyz")))
                .execute(getEngine());
        final AbstractSortOrderingSpecification sos = insertTable.text.desc();
        try {
            final List<String> list = insertTable.text.orderBy(sos.nullsFirst()).list(getEngine());
            assertEquals(Arrays.asList(null, "xyz", "abc"), list);
        } catch (SQLException e) {
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_nullsLast_() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("abc")))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull()))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("xyz")))
                .execute(getEngine());
        final AbstractSortOrderingSpecification sos = insertTable.text.desc();
        try {
            final List<String> list = insertTable.text.orderBy(sos.nullsLast()).list(getEngine());
            assertEquals(Arrays.asList( "xyz", "abc", null), list);
        } catch (SQLException e) {
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        final InsertTable insertTable = new InsertTable();
        insertTable.delete().execute(getEngine());
        insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("abc")))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull()))
                .execute(getEngine());
        insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("xyz")))
                .execute(getEngine());
        final AbstractSortOrderingSpecification sos = insertTable.text.desc();
        final List<String> list = insertTable.text.orderBy(sos).list(getEngine());
        final List<String> expected;
        if (NULLS_FIRST_DEFAULT.contains(getDatabaseName())) {
            // nulls will be last: desc()
            expected = Arrays.asList("xyz", "abc", null);
        } else {
            expected = Arrays.asList(null, "xyz", "abc");
        }
        assertEquals(expected, list);
    }
}
