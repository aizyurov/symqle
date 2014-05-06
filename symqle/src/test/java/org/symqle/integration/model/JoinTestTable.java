package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public abstract class JoinTestTable extends TableOrView {

    public Column<Integer> id() {
        return defineColumn(CoreMappers.INTEGER, "id");
    }

    public Column<String> text() {
        return defineColumn(CoreMappers.STRING, "text");
    }
}
