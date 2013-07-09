package org.symqle.common;

import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public abstract class NiceSql implements Sql {

    @Override
    public String toString() {
        final String s0 = REPEATING_WHITESPACE.matcher(sql()).replaceAll(" ");
        final String s1 = UNNEEDED_SPACE_AFTER.matcher(s0).replaceAll("");
        final String s2 = UNNEEDED_SPACE_BEFORE.matcher(s1).replaceAll("");
        return s2.trim();
    }

    private final static Pattern REPEATING_WHITESPACE = Pattern.compile("\\s+");
    private final static Pattern UNNEEDED_SPACE_AFTER = Pattern.compile("(?<=[(.]) ");
    private final static Pattern UNNEEDED_SPACE_BEFORE = Pattern.compile(" (?=[().,])");
}
