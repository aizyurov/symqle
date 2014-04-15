package org.symqle.integration.model;

import org.symqle.sql.Column;
import org.symqle.sql.Mappers;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class Attribute extends Table {

    @Override
    public String getTableName() {
        return "attribute";
    }

    public Column<Long> itemId = defineColumn(Mappers.LONG, "item_id");

    public Column<String> name = defineColumn(Mappers.STRING, "name");

    public Column<String> value = defineColumn(Mappers.STRING, "value");
}
