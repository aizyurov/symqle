package org.symqle.integration.model;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class DeleteMaster extends Table {

    public DeleteMaster() {
        super("delete_master");
    }

    public Column<Integer> masterId = defineColumn(Mappers.INTEGER, "master_id");
    public Column<String> description = defineColumn(Mappers.STRING, "description");
}
