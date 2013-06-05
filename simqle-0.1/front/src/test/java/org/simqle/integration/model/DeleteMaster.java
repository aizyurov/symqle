package org.simqle.integration.model;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.Table;

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
