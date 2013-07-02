package org.symqle.integration.model;

import org.symqle.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class One extends TableOrView {

    public One() {
        super("one");
    }

    public final Column<Integer> id = defineColumn(Mappers.INTEGER, "id");
}
