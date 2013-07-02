package org.symqle.integration.model;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

/**
 * @author lvovich
 */
public class Country extends Table {

    public Country() {
        super("country");
    }

    public final Column<Integer> countryId = defineColumn(Mappers.INTEGER, "country_id");
    public final Column<String> name = defineColumn(Mappers.STRING, "name");
    public final Column<String> code = defineColumn(Mappers.STRING, "code");


}
