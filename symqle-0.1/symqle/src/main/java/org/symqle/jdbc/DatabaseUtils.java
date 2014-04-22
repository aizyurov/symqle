/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

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

/**
 * Internal utility class.
 * @author lvovich
 */
final class DatabaseUtils {

    private DatabaseUtils() {}
    static { new DatabaseUtils(); }

    /**
     * Reads database name from metadata.
     * @param dataSource the database to connect
     * @return database bane as reported by {@link java.sql.DatabaseMetaData#getDatabaseProductName()}
     * @throws SQLException
     */
    static String getDatabaseName(final DataSource dataSource) throws SQLException {
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

    /**
     * Find suitable dialect for this database.
     * @param databaseName target database.
     * @return supporting dialect; GenericDialect if the database is unnown.
     */
    static Dialect getDialect(final String databaseName) {
        final String className = dialects.getProperty(databaseName);
        return Bug.ifFails(new Callable<Dialect>() {
            @Override
            public Dialect call() throws Exception {
                return className != null ? (Dialect) Class.forName(className).newInstance() : new GenericDialect();
            }
        });
    }


    /**
     * Creates a proper Connector for this database.
     * @param connector original connector.
     * @param databaseName target database
     * @return original connector or wrapper as needed
     */
    static Connector wrap(final Connector connector, final String databaseName) {
        final String className = connectors.getProperty(databaseName);
        if (className == null) {
            return connector;
        }
        return Bug.ifFails(new Callable<Connector>() {
            @Override
            public Connector call() throws Exception {
                final ConnectorWrapper wrapper = (ConnectorWrapper) Class.forName(className).newInstance();
                return wrapper.wrap(connector);
            }
        });
    }


    /**
     * Read properties from resource.
     * @param path should point to valid resource with Properties format.
     * @return loaded properties
     */
    static Properties readProperties(final String path) {
        return Bug.ifFails(new Callable<Properties>() {
            @Override
            public Properties call() throws Exception {
                final InputStream inputStream = DatabaseUtils.class.getClassLoader().getResourceAsStream(path);
                Bug.reportIfNull(inputStream);
                try {
                    final Properties properties = new Properties();
                    properties.load(inputStream);
                    return properties;
                } finally {
                    inputStream.close();
                }
            }
        });
    }
}
