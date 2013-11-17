package org.symqle.coretest;

import org.symqle.common.SqlContext;
import org.symqle.jdbc.Configuration;
import org.symqle.jdbc.UpdatableConfiguration;
import org.symqle.sql.Dialect;

/**
 * @author lvovich
 */
public class SqlContextUtil {

    private SqlContextUtil() {
    }

    public static SqlContext allowNoTablesContext() {
        final SqlContext context = new SqlContext();
        context.set(Dialect.class, new OracleLikeDialect());
        UpdatableConfiguration configuration = new UpdatableConfiguration();
        configuration.setNoFromOk(true);
        context.set(Configuration.class, configuration);
        return context;
    }
}
