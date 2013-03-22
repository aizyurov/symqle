package org.simqle.integration.model;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.Table;

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
