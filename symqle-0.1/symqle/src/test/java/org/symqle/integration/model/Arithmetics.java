package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class Arithmetics extends TableOrView {

    @Override
    public String getTableName() {
        return "arithmetics";
    }

    public Column<Integer> leftInt() {
        return defineColumn(CoreMappers.INTEGER, "leftInt");
    }

    public Column<Integer> rightInt() {
        return defineColumn(CoreMappers.INTEGER, "rightInt");
    }

    public Column<Integer> leftDouble() {
        return defineColumn(CoreMappers.INTEGER, "leftDouble");
    }

    public Column<Integer> rightDouble() {
        return defineColumn(CoreMappers.INTEGER, "rightDouble");
    }
}
