package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class DeleteMaster extends Table {

    @Override
    public String getTableName() {
        return "delete_master";
    }

    public Column<Integer> masterId = defineColumn(CoreMappers.INTEGER, "master_id");
    public Column<String> description = defineColumn(CoreMappers.STRING, "description");
}
