package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class InsertTable extends Table {

    @Override
    public String getTableName() {
        return "insert_test";
    }

    public final Column<Integer> id = defineColumn(CoreMappers.INTEGER, "id");
    public final Column<String> text = defineColumn(CoreMappers.STRING, "text");
}
