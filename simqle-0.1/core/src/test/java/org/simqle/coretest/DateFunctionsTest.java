package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.Simqle;
import org.simqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class DateFunctionsTest extends SqlTestCase {

    public void testCurrentDate() throws Exception {
        final String sql = Simqle.get().currentDate().pair(new Dual().dummy).show();
        assertSimilar("SELECT CURRENT_DATE AS C0, T1.dummy AS C1 FROM dual AS T1", sql);
    }

    public void testCurrentTimestamp() throws Exception {
        final String sql = Simqle.get().currentTimestamp().pair(new Dual().dummy).show();
        assertSimilar("SELECT CURRENT_TIMESTAMP AS C0, T1.dummy AS C1 FROM dual AS T1", sql);
    }

    public void testCurrentTime() throws Exception {
        final String sql = Simqle.get().currentTime().pair(new Dual().dummy).show();
        assertSimilar("SELECT CURRENT_TIME AS C0, T1.dummy AS C1 FROM dual AS T1", sql);
    }

    private static class Dual extends TableOrView {

        private Dual() {
            super("dual");
        }

        public final Column<String> dummy = defineColumn(Mappers.STRING, "dummy");
    }
}
