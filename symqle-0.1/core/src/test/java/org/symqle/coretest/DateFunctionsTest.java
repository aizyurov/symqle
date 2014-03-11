package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.jdbc.Option;
import org.symqle.sql.Symqle;

/**
 * @author lvovich
 */
public class DateFunctionsTest extends SqlTestCase {

    public void testCurrentDate() throws Exception {
        final String sql = Symqle.currentDate().showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT CURRENT_DATE AS C0 FROM dual AS T1", sql);
        assertEquals(CoreMappers.DATE, Symqle.currentDate().getMapper());
    }

    public void testCurrentTimestamp() throws Exception {
        final String sql = Symqle.currentTimestamp().showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT CURRENT_TIMESTAMP AS C0 FROM dual AS T1", sql);
        assertEquals(CoreMappers.TIMESTAMP, Symqle.currentTimestamp().getMapper());
    }

    public void testCurrentTime() throws Exception {
        final String sql = Symqle.currentTime().showQuery(new OracleLikeDialect(), Option.allowNoTables(true));
        assertSimilar("SELECT CURRENT_TIME AS C0 FROM dual AS T1", sql);
        assertEquals(CoreMappers.TIME, Symqle.currentTime().getMapper());
    }

}
