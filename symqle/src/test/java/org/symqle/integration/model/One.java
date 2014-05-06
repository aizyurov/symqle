package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class One extends TableOrView {

    @Override
    public String getTableName() {
        return "one";
    }

    public final Column<Integer> id = defineColumn(CoreMappers.INTEGER, "id");
}
