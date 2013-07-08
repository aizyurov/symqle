package org.symqle.integration.model;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class JoinTestTable extends TableOrView {

    public JoinTestTable(final String name) {
        super(name);
    }

    public Column<Integer> id() {
        return defineColumn(Mappers.INTEGER, "id");
    }

    public Column<String> text() {
        return defineColumn(Mappers.STRING, "text");
    }
}
