package org.symqle.integration.model;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class BigTable extends TableOrView {

    public BigTable() {
        super("big_table");
    }

    public final Column<Integer> num = defineColumn(Mappers.INTEGER, "num");
}
