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

    public String generate(final String prefix) {
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
