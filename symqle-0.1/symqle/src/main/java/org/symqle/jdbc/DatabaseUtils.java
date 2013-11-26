package org.symqle.jdbc;

import org.symqle.misc.ResourceUtils;
import org.symqle.misc.SafeCallable;
import org.symqle.sql.Dialect;
import org.symqle.sql.GenericDialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;
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

    private final static Properties dialects = ResourceUtils.readProperties("symqle.dialects");
    private final static Properties connectors = ResourceUtils.readProperties("symqle.connectors");

    public static Dialect getDialect(final String databaseName) {
        final String className = dialects.getProperty(databaseName);
        final AtomicReference<Dialect> reference = new AtomicReference<Dialect>();
        new SafeCallable() {
            @Override
            public Void call() throws Exception {
                reference.set(className != null ? (Dialect) Class.forName(className).newInstance() : new GenericDialect());
                return null;
            }
        }.run();
        return reference.get();
    }



    public static Connector wrap(final Connector connector, final String databaseName) {
        final String className = connectors.getProperty(databaseName);
        if (className == null) {
            return connector;
        }
        final AtomicReference<Connector> reference = new AtomicReference<Connector>();
        new SafeCallable() {
            @Override
            public Void call() throws Exception {
                final ConnectorWrapper wrapper = (ConnectorWrapper) Class.forName(className).newInstance();
                reference.set(wrapper.wrap(connector));
                return null;
            }
        }.run();
        return reference.get();
    }



}
