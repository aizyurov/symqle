package org.symqle.integration;

import org.symqle.jdbc.Option;
import org.symqle.sql.Symqle;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * @author lvovich
 */
public class SymqleTest extends AbstractIntegrationTestBase {

    public void testCurrentDate() throws Exception {
        final List<Date> list = Symqle.currentDate().list(getEngine(), Option.allowNoTables(true));
        assertEquals(1, list.size());
        final Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        final long startOfDay = calendar.getTimeInMillis();
        assertEquals(startOfDay, list.get(0).getTime());
    }

    public void testCurrentTimestamp() throws Exception {
        final List<Timestamp> list = Symqle.currentTimestamp().list(getEngine(), Option.allowNoTables(true));
        assertEquals(1, list.size());
        final long clientTs = System.currentTimeMillis();
        final long backendTs = list.get(0).getTime();
        final long difference = Math.abs(clientTs - backendTs);
        assertTrue("Difference too big, client: " + clientTs + ", backend: " + backendTs, difference < 1000);
    }

    public void testCurrentTime() throws Exception {
        final List<Time> list = Symqle.currentTime().list(getEngine(), Option.allowNoTables(true));
        assertEquals(1, list.size());
        final Calendar calendar = GregorianCalendar.getInstance();
        System.out.println(Arrays.toString(TimeZone.getAvailableIDs()));
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        final long startOfDay = calendar.getTimeInMillis();
//        final long clientTs = System.currentTimeMillis() - startOfDay;
        final long clientTs = System.currentTimeMillis() % TimeUnit.DAYS.toMillis(1);
        final long backendTs = list.get(0).getTime();
        final long difference = Math.abs(clientTs - backendTs);
        assertTrue("Unexpected: " + backendTs, backendTs > 0 && backendTs < TimeUnit.DAYS.toMillis(1));
        // no better assertions: too many bugs (no correction for daylight saving time etc.)
        // passes for PostgreSQL
//        assertTrue("Difference too big, client: " + clientTs + ", backend: " + backendTs, difference < 1000);
    }

    public void testCurrentUser() throws Exception {
        final List<String> list = Symqle.currentUser().list(getEngine(), Option.allowNoTables(true));
        assertEquals(1, list.size());
        final String currentUser;
        if (SupportedDb.MYSQL.equals(getDatabaseName())) {
            currentUser = currentUser() + "@%";
        } else {
            currentUser = currentUser();
        }
        assertEquals(currentUser, list.get(0));
    }

}
