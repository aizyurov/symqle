package org.simqle.integration.model;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class One extends TableOrView {

    public One() {
        super("one");
    }

    public final Column<Integer> id = defineColumn(Mappers.INTEGER, "id");
}
