package org.symqle.integration.model;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class GeneratedKeys extends Table {

    public GeneratedKeys() {
        super("generated_keys");
    }

    public Column<Integer> id() {
        return defineColumn(Mappers.INTEGER, "id");
    }

    public Column<String> text() {
        return defineColumn(Mappers.STRING, "text");
    }

}
