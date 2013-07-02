package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.Symqle;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class DateFunctionsTest extends SqlTestCase {

    public void testCurrentDate() throws Exception {
        final String sql = Symqle.get().currentDate().pair(new Dual().dummy).show();
        assertSimilar("SELECT CURRENT_DATE AS C0, T1.dummy AS C1 FROM dual AS T1", sql);
    }

    public void testCurrentTimestamp() throws Exception {
        final String sql = Symqle.get().currentTimestamp().pair(new Dual().dummy).show();
        assertSimilar("SELECT CURRENT_TIMESTAMP AS C0, T1.dummy AS C1 FROM dual AS T1", sql);
    }

    public void testCurrentTime() throws Exception {
        final String sql = Symqle.get().currentTime().pair(new Dual().dummy).show();
        assertSimilar("SELECT CURRENT_TIME AS C0, T1.dummy AS C1 FROM dual AS T1", sql);
    }

    private static class Dual extends TableOrView {

        private Dual() {
            super("dual");
        }

        public final Column<String> dummy = defineColumn(Mappers.STRING, "dummy");
    }
}
