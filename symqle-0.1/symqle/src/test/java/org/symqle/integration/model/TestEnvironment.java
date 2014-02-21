package org.symqle.integration.model;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * @author lvovich
 */
public interface TestEnvironment {
    DataSource prepareDataSource(Properties properties) throws Exception;
}
