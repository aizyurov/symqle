package org.symqle;

import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

/**
 * @author lvovich
 */
public class MetadataTest extends TestCase {

    private final String url = "jdbc:derby:memory:symqle";

    public void testFunctions() throws Exception {
        final Connection connection = DriverManager.getConnection(url + ";create=true");
        final DatabaseMetaData metaData = connection.getMetaData();
        final ResultSet functions = metaData.getColumns("%", "%", "%", "%");
        while (functions.next()) {
            System.out.println("TABLE_CAT: " + functions.getString("TABLE_CAT"));
            System.out.println("TABLE_SCHEM: " + functions.getString("TABLE_SCHEM"));
            System.out.println("TABLE_NAME: " + functions.getString("TABLE_NAME"));
            System.out.println("COLUMN_NAME: " + functions.getString("COLUMN_NAME"));
            System.out.println("===");
        }
        functions.close();
    }

}
