package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class Country extends Table {

    @Override
    public String getTableName() {
        return "country";
    }

    public final Column<Integer> countryId = defineColumn(CoreMappers.INTEGER, "country_id");
    public final Column<String> name = defineColumn(CoreMappers.STRING, "name");
    public final Column<String> code = defineColumn(CoreMappers.STRING, "code");


}
