package org.symqle.coretest;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lvovich
 */
public abstract class SqlTestCase extends TestCase {
    protected final void assertSimilar(String expected, String actual) {
        final String asPattern = "([A-Za-z]+[0-9]+)";
        final Pattern aliasMatcher = Pattern.compile(asPattern);
        final Matcher matcher = aliasMatcher.matcher(expected);
        final List<String> expectedAliases = new ArrayList<String>();
        StringBuilder patternBuilder = new StringBuilder();
        int lastMatchEnd = 0;
        while(matcher.find()) {
            expectedAliases.add(matcher.group(1));
            patternBuilder.append(escapeSpecialSymbols(expected.substring(lastMatchEnd, matcher.start())));
            patternBuilder.append(asPattern);
            lastMatchEnd = matcher.end();
        }
        if (lastMatchEnd < expected.length()) {
            patternBuilder.append(escapeSpecialSymbols(expected.substring(lastMatchEnd, expected.length())));
        }
        final Map<String, String> knownMappings = new HashMap<String, String>();
        Pattern sqlPattern = Pattern.compile(patternBuilder.toString());
        final Matcher actualMatcher = sqlPattern.matcher(actual);
        assertTrue("Pattern does not match, actual: "+actual, actualMatcher.matches());
        for (int i=1; i<= actualMatcher.groupCount(); i++) {
            final String expectedAlias = expectedAliases.get(i-1);
            final String mapped = knownMappings.get(expectedAlias);
            if (mapped != null) {
                assertEquals("Group " + i +" does not match: expected "+ mapped +" but was " + actualMatcher.group(i),
                        mapped, actualMatcher.group(i));
            } else {
                knownMappings.put(expectedAlias, mapped);
            }
        }
    }

    public static String escapeSpecialSymbols(String source) {
        StringBuilder builder = new StringBuilder();
        for (int i=0; i<source.length(); i++) {
            final char c = source.charAt(i);
            switch (c) {
                case '*':
                case '.':
                case '+':
                case '?':
                case '(':
                case ')':
                case '|':
                    builder.append('\\');
                // and fall through
                default: builder.append(c);
            }
        }
        return builder.toString();
    }
}
