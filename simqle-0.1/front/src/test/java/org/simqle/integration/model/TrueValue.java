package org.simqle.integration.model;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class TrueValue extends TableOrView {

    public TrueValue() {
        super("true_value");
    }

    public Column<Boolean> value = defineColumn(Mappers.BOOLEAN, "value");
}
