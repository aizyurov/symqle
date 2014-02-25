package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class DeleteDetail extends Table {

    public DeleteDetail() {
        super("delete_detail");
    }

    public Column<Integer> detailId = defineColumn(CoreMappers.INTEGER, "detail_id");
    public Column<String> detail = defineColumn(CoreMappers.STRING, "detail");
    public Column<Integer> masterId = defineColumn(CoreMappers.INTEGER, "master_id");
}
