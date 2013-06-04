package org.simqle.integration.model;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class MyDual extends TableOrView {

    public MyDual() {
        super("my_dual");
    }

    public final Column<String> dummy = defineColumn(Mappers.STRING, "dummy");
}
