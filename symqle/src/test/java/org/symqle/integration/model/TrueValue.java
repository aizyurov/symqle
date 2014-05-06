package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class TrueValue extends TableOrView {

    @Override
    public String getTableName() {
        return "true_value";
    }

    public Column<Boolean> value = defineColumn(CoreMappers.BOOLEAN, "value");
}
