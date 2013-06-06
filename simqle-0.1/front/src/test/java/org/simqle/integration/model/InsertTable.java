package org.simqle.integration.model;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.Table;

/**
 * @author lvovich
 */
public class InsertTable extends Table {

    public InsertTable() {
        super("insert_test");
    }

    public final Column<Integer> id = defineColumn(Mappers.INTEGER, "id");
    public final Column<String> text = defineColumn(Mappers.STRING, "text");
}
