package org.symqle.jdbc;

import org.symqle.common.Bug;
import org.symqle.sql.Dialect;
import org.symqle.sql.GenericDialect;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lvovich
 */
public final class DatabaseUtils {

    private DatabaseUtils() {}
    static { new DatabaseUtils(); }

    public static String getDatabaseName(final DataSource dataSource) throws SQLException {
        final Connection connection = dataSource.getConnection();
        try {
            final DatabaseMetaData metaData = connection.getMetaData();
            return metaData.getDatabaseProductName();
        } finally {
            connection.close();
        }
    }

    private final static Properties dialects = readProperties("symqle.dialects");
    private final static Properties connectors = readProperties("symqle.connectors");

    public static Dialect getDialect(final String databaseName) {
        final String className = dialects.getProperty(databaseName);
        return Bug.ifFails(new Callable<Dialect>() {
            @Override
            public Dialect call() throws Exception {
                return className != null ? (Dialect) Class.forName(className).newInstance() : new GenericDialect();
            }
        });
    }



    public static Connector wrap(final Connector connector, final String databaseName) {
        final String className = connectors.getProperty(databaseName);
        if (className == null) {
            return connector;
        }
        final AtomicReference<Connector> reference = new AtomicReference<Connector>();
        return Bug.ifFails(new Callable<Connector>() {
            @Override
            public Connector call() throws Exception {
                final ConnectorWrapper wrapper = (ConnectorWrapper) Class.forName(className).newInstance();
                return wrapper.wrap(connector);
            }
        });
    }


    public static Properties readProperties(final String path) {
        return Bug.ifFails(new Callable<Properties>() {
            @Override
            public Properties call() throws Exception {
                try (InputStream inputStream = DatabaseUtils.class.getClassLoader().getResourceAsStream(path)) {
                    Bug.reportIfNull(inputStream);
                    final Properties properties = new Properties();
                    properties.load(inputStream);
                    return properties;
                }
            }
        });
    }
}
