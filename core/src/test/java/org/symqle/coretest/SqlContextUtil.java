package org.symqle.coretest;

import org.symqle.common.SqlContext;
import org.symqle.querybuilder.Configuration;
import org.symqle.querybuilder.UpdatableConfiguration;
import org.symqle.sql.Dialect;

/**
 * @author lvovich
 */
public class SqlContextUtil {

    private SqlContextUtil() {
    }

    public static SqlContext allowNoTablesContext() {
        UpdatableConfiguration configuration = new UpdatableConfiguration();
        configuration.setNoFromOk(true);
        final SqlContext context = new SqlContext.Builder().put(Dialect.class, new OracleLikeDialect()).
                put(Configuration.class, configuration).toSqlContext();
        return context;
    }
}
