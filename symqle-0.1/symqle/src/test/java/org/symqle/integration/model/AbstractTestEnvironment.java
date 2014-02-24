package org.symqle.integration.model;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author lvovich
 */
public abstract class AbstractTestEnvironment implements TestEnvironment {
    protected void initDatabase(final Connection connection, String resource) throws Exception {
    final BufferedReader reader = new BufferedReader(
            new InputStreamReader(DerbyEnvironment.class.getClassLoader().getResourceAsStream(resource)));
    try {
        final StringBuilder builder = new StringBuilder();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.trim().equals("")) {
                    final String sql = builder.toString();
                    builder.setLength(0);
                    if (sql.trim().length()>0) {
                        System.out.println(sql);
                        final PreparedStatement preparedStatement = connection.prepareStatement(sql);
                        try {
                            preparedStatement.executeUpdate();
                        } catch (SQLException e) {
                            e.printStackTrace();
                            throw e;
                        }
                        preparedStatement.close();
                    }
                } else {
                    builder.append(" ").append(line);
                }
            }
        final String sql = builder.toString();
        if (sql.trim().length()>0) {
            System.out.println(sql);
            final PreparedStatement preparedStatement = connection.prepareStatement(sql);
            try {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
                throw e;
            }
            preparedStatement.close();
        }
    } finally {
        reader.close();
    }
    }
}
