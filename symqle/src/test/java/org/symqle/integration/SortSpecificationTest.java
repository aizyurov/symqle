package org.symqle.integration;

import org.symqle.integration.model.InsertTable;
import org.symqle.sql.AbstractSortSpecification;
import org.symqle.testset.AbstractSortSpecificationTestSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class SortSpecificationTest extends AbstractIntegrationTestBase implements AbstractSortSpecificationTestSet {

    @Override
    public void test_orderBy_QueryExpressionBody_SortSpecification_SortSpecification_1() throws Exception {
        try {
            final InsertTable insertTable = new InsertTable();
            insertTable.delete().execute(getEngine());
            insertTable.insert(insertTable.id.set(1).also(insertTable.text.set("abc")))
                    .execute(getEngine());
            insertTable.insert(insertTable.id.set(2).also(insertTable.text.setNull()))
                    .execute(getEngine());
            insertTable.insert(insertTable.id.set(3).also(insertTable.text.set("xyz")))
                    .execute(getEngine());
            final AbstractSortSpecification spec = insertTable.text.desc().nullsFirst();
            final List<String> list = insertTable.text.orderBy(spec).list(getEngine());
            assertEquals(Arrays.asList(null, "xyz", "abc"), list);
        } catch (SQLException e) {
            // MySQL does not support NULLS FIRST
            expectSQLException(e, SupportedDb.MYSQL);
        }
    }
}
