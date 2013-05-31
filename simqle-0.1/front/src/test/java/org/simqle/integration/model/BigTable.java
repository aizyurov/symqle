package org.simqle.integration.model;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class BigTable extends TableOrView {

    public BigTable() {
        super("big_table");
    }

    public final Column<Integer> num = defineColumn(Mappers.INTEGER, "num");
}
