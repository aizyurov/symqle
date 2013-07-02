package org.symqle.integration.model;

import org.symqle.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

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
