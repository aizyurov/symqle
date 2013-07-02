package org.symqle.integration.model;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class TrueValue extends TableOrView {

    public TrueValue() {
        super("true_value");
    }

    public Column<Boolean> value = defineColumn(Mappers.BOOLEAN, "value");
}
