package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.Symqle;

/**
 * @author lvovich
 */
public class DateFunctionsTest extends SqlTestCase {

    public void testCurrentDate() throws Exception {
        final String sql = Symqle.currentDate().show(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT CURRENT_DATE AS C0 FROM dual AS T1", sql);
        assertEquals(Mappers.DATE, Symqle.currentDate().getMapper());
    }

    public void testCurrentTimestamp() throws Exception {
        final String sql = Symqle.currentTimestamp().show(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT CURRENT_TIMESTAMP AS C0 FROM dual AS T1", sql);
        assertEquals(Mappers.TIMESTAMP, Symqle.currentTimestamp().getMapper());
    }

    public void testCurrentTime() throws Exception {
        final String sql = Symqle.currentTime().show(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT CURRENT_TIME AS C0 FROM dual AS T1", sql);
        assertEquals(Mappers.TIME, Symqle.currentTime().getMapper());
    }

}
