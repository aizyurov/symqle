package org.symqle.integration.model;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class Arithmetics extends TableOrView {

    public Arithmetics() {
        super("arithmetics");
    }

    public Column<Integer> leftInt() {
        return defineColumn(Mappers.INTEGER, "leftInt");
    }

    public Column<Integer> rightInt() {
        return defineColumn(Mappers.INTEGER, "rightInt");
    }

    public Column<Integer> leftDouble() {
        return defineColumn(Mappers.INTEGER, "leftDouble");
    }

    public Column<Integer> rightDouble() {
        return defineColumn(Mappers.INTEGER, "rightDouble");
    }
}
