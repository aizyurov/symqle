package org.symqle.integration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

/**
 * @author lvovich
 */
public class AllTypesDebug extends AbstractIntegrationTestBase {

    public void testTypes() throws Exception {
        final DataSource dataSource = getDataSource();
        Connection conn = dataSource.getConnection();
        final DatabaseMetaData metaData = conn.getMetaData();
        final ResultSet rs = metaData.getColumns(null, null, "all_types", null);
        while (rs.next()) {
            System.out.println(rs.getString("COLUMN_NAME") + " " +rs.getInt("DATA_TYPE"));
        }
        rs.close();
        conn.close();
    }
}
