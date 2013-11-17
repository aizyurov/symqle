package org.symqle.coretest;

import org.symqle.sql.GenericDialect;

/**
 * @author lvovich
 */
public class OracleLikeDialect extends GenericDialect {

    @Override
    public String fallbackTableName() {
        return "dual";
    }

}
