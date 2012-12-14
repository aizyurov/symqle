package org.simqle.sql;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        final List<String> aliases = new ArrayList<String>();
        while(matcher.find()) {
            aliases.add(matcher.group(1));
        }
        matcher.replaceAll(asPattern);
        final Map<String, List<Integer>> actualAliases = new HashMap<String, List<Integer>>();
        final Matcher actualMatcher = aliasMatcher.matcher(actual);
        int i = 0;
        while (actualMatcher.find()) {
            final String alias = actualMatcher.group(1);
            List<Integer> positions = actualAliases.get(alias);
            if (positions == null) {
                positions = new ArrayList<Integer>();
                actualAliases.put(alias, positions);
            }
            positions.add(i++);
        }
        final Set<String> usedExpectedAliases = new HashSet<String>();
        final Map<String, String> actualAliasToExpected = new HashMap<String, String>();
        for (String alias: actualAliases.keySet()) {
            final HashSet<String> mapping = new HashSet<String>();
            for (Integer position: actualAliases.get(alias)) {
                if (position >= aliases.size()) {
                    fail("Does not match: \""+expected+"\" to \""+actual+"\"");
                }
                mapping.add(aliases.get(position));
            }
            if (mapping.size() != 1) {
                fail("Does not match: \""+expected+"\" to \""+actual+"\"");
            } else {
                final String expectedAlias = mapping.iterator().next();
                if (!usedExpectedAliases.add(expectedAlias)) {
                    fail("Does not match: \""+expected+"\" to \""+actual+"\"");
                }
                actualAliasToExpected.put(alias, expectedAlias);
            }
        }

        String actualWithReplacedAliases = actual;
        for (String alias: actualAliasToExpected.keySet()) {
            actualWithReplacedAliases = actualWithReplacedAliases.replaceAll(alias, actualAliasToExpected.get(alias));
        }
        assertEquals(expected, actualWithReplacedAliases);
    }
}
