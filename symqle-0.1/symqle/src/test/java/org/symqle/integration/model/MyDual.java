package org.symqle.integration.model;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class MyDual extends TableOrView {

    public MyDual() {
        super("my_dual");
    }

    public final Column<String> dummy = defineColumn(Mappers.STRING, "dummy");
}
