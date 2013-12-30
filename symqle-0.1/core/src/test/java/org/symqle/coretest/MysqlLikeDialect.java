package org.symqle.coretest;

import org.symqle.sql.GenericDialect;

/**
 * @author lvovich
 */
public class MysqlLikeDialect extends GenericDialect {

    @Override
    public String fallbackTableName() {
        return null;
    }
}
