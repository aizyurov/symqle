package org.symqle.integration.model;

import org.symqle.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class DeleteDetail extends Table {

    public DeleteDetail() {
        super("delete_detail");
    }

    public Column<Integer> detailId = defineColumn(Mappers.INTEGER, "detail_id");
    public Column<String> detail = defineColumn(Mappers.STRING, "detail");
    public Column<Integer> masterId = defineColumn(Mappers.INTEGER, "master_id");
}
