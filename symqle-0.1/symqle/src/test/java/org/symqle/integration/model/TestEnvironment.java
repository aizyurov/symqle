package org.symqle.integration.model;

import javax.sql.DataSource;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lvovich
 */
public interface TestEnvironment {
    DataSource prepareDataSource(Properties properties, AtomicReference<String> userNameHolder) throws Exception;
}
