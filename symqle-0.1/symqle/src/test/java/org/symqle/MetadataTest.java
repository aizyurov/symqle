package org.symqle;

import com.mysql.jdbc.Driver;
import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

/**
 * @author lvovich
 */
public class MetadataTest extends TestCase {

//    private final String url = "jdbc:derby:memory:symqle";
    private final String url = "jdbc:mysql://localhost:3306/simqle";

    public void testFunctions() throws Exception {
        DriverManager.registerDriver(new Driver());
        final Connection connection = DriverManager.getConnection(url, "simqle", "simqle");
        final DatabaseMetaData metaData = connection.getMetaData();
        final ResultSet columns = metaData.getColumns(null, null, null, null);
        while (columns.next()) {
            System.out.println("TABLE_CAT: " + columns.getString("TABLE_CAT"));
            System.out.println("TABLE_SCHEM: " + columns.getString("TABLE_SCHEM"));
            System.out.println("TABLE_NAME: " + columns.getString("TABLE_NAME"));
            System.out.println("COLUMN_NAME: " + columns.getString("COLUMN_NAME"));
            System.out.println("===");
        }
        columns.close();
    }

}
