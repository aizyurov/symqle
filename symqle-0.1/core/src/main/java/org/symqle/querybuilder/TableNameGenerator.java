package org.symqle.querybuilder;

import org.symqle.common.Bug;

import java.util.HashSet;
import java.util.Set;

/**
 * @author lvovich
 */
public class TableNameGenerator {

    private final static int MAX_PREFIX_LENGTH = 20;

    private final Set<String> used = new HashSet<String>();

    public String generate(final String suggestedPrefix) {
        final String trimmedPrefix = suggestedPrefix.trim();
        final int spaceIndex = trimmedPrefix.indexOf(" ");
        final String truncatedPrefix = spaceIndex > 0 ? trimmedPrefix.substring(0, spaceIndex) : trimmedPrefix;
        final String prefix = truncatedPrefix.replaceAll("[^a-zA-Z0-9_]", "");
        final String shortPrefix =
                (prefix.length() < MAX_PREFIX_LENGTH ? prefix: prefix.substring(0, MAX_PREFIX_LENGTH))
                        .toUpperCase();
        int i=0;
        String name = shortPrefix + i;
        while (used.contains(name)) {
            name = shortPrefix + (++i);

        }
        used.add(name);
        return name;
    }

    public void force(final String name) {
        Bug.reportIf(used.contains(name));
        used.add(name);
    }
}
