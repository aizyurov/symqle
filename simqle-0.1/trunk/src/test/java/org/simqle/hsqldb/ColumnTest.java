package org.simqle.hsqldb;

import junit.framework.TestCase;
import org.hsqldb.jdbcDriver;
import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.TableOrView;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author lvovich
 */
public class ColumnTest extends TestCase {

    static {
        try {
            DriverManager.registerDriver(new jdbcDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Internal error", e);
        }
    }

    private DataSource dataSource = new DriverManagerDataSource("jdbc:hsqldb:mem:simqle", "SA", "");

    public void testDbConnection() throws Exception {
        final Connection connection = dataSource.getConnection();
        final PreparedStatement createStatement = connection.prepareStatement("CREATE TABLE person (id BIGINT, name VARCHAR, age INTEGER, alive BOOLEAN)");
        createStatement.executeUpdate();
        createStatement.close();
        final PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM person");
        final ResultSet resultSet = selectStatement.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();
        selectStatement.close();

        final DatabaseMetaData metaData = connection.getMetaData();
        final ResultSet rs = metaData.getTables(null, "PUBLIC", null, null);
        final ResultSetMetaData rsMetadata = rs.getMetaData();
        System.out.println("========");
        final int columnCount = rsMetadata.getColumnCount();
        while(rs.next()) {
            for (int i=1; i<= columnCount; i++) {
                System.out.println(rs.getString(i));
            }
            System.out.println("========");
        }
        rs.close();
        connection.close();

    }

    private class Person extends TableOrView {
        private Person() {
            super("person");
        }

        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Integer> age = defineColumn(Mappers.INTEGER, "age");
        public Column<Boolean> alive = defineColumn(Mappers.BOOLEAN, "alive");
    }
}
