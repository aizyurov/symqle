package org.symqle.integration.model;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class Item extends Table {

    @Override
    public String getTableName() {
        return "item";
    }

    public Column<Long> id = defineColumn(Mappers.LONG, "id");

    public Column<String> name = defineColumn(Mappers.STRING, "name");

}
