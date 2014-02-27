package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class GeneratedKeysTable extends Table {

    public GeneratedKeysTable() {
        super("generated_keys");
    }

    public Column<Integer> id() {
        return defineColumn(CoreMappers.INTEGER, "id");
    }

    public Column<String> text() {
        return defineColumn(CoreMappers.STRING, "text");
    }

}
