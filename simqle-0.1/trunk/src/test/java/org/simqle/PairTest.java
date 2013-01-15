package org.simqle;

import junit.framework.TestCase;

/**
 * @author lvovich
 */
public class PairTest extends TestCase {

    public void testEq() {
        final Pair<String, String> ab1 = Pair.of("a", "b");
        final Pair<String, String> ab2 = Pair.of("a", "b");
        final Pair<String, String> az = Pair.of("a", "z");
        final Pair<String, String> xb = Pair.of("x", "b");
        final Pair<String, String> an1 = Pair.of("a", null);
        final Pair<String, String> an2 = Pair.of("a", null);
        final Pair<String, String> xn = Pair.of("x", null);
        final Pair<String, String> nb1 = Pair.of(null, "b");
        final Pair<String, String> nb2 = Pair.of(null, "b");
        final Pair<String, String> nz = Pair.of(null, "z");
        final Pair<String, String> nn1 = Pair.of(null, null);
        final Pair<String, String> nn2 = Pair.of(null, null);

        assertEquals(ab1, ab2);
        assertEquals(an1, an2);
        assertEquals(nb1, nb2);
        assertEquals(nn1, nn2);

        assertEquals(ab1.hashCode(), ab2.hashCode());
        assertEquals(an1.hashCode(), an2.hashCode());
        assertEquals(nb1.hashCode(), nb2.hashCode());
        assertEquals(nn1.hashCode(), nn2.hashCode());

        assertNotEquals(ab1, az);
        assertNotEquals(ab1, xb);
        assertNotEquals(ab1, an1);
        assertNotEquals(ab1, nb1);
        assertNotEquals(ab1, nz);
        assertNotEquals(ab1, nn1);

        assertEquals(ab1, ab1);
        assertNotEquals(ab1, "a");
        assertNotEquals(ab1, null);


        assertNotEquals(nb1, nz);
        assertNotEquals(nb1, nn1);
        assertNotEquals(an1, xn);
        assertNotEquals(an1, nn1);
        assertNotEquals(nn1, an1);
        assertNotEquals(nn1, nb1);

        assertEquals(ab1.getFirst(), "a");
        assertEquals(ab1.getSecond(), "b");
    }


    private void assertNotEquals(Object a, Object b) {
        if (a.equals(b)) {
            fail("Expected "+ a + " to be not equal to "+b);
        }
    }
}
