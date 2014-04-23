package org.symqle.querybuilder;

import org.symqle.common.Bug;

import java.util.HashSet;
import java.util.Set;

/**
 * @author lvovich
 */
public class TableNameGenerator {

    private static final int MAX_PREFIX_LENGTH = 20;

    private final Set<String> used = new HashSet<String>();

    /**
     * Generate a unique name.
     * Suggested prefix is used (punctuation removed, may be truncated, converted to upper case).
     * A number is added to formatted prefix to ensure uniqueness.
     * @param suggestedPrefix any text (at least one alphanumeric character is required)
     * @return unique name
     */
    public final String generate(final String suggestedPrefix) {
        final String identifiersOnlyPrefix = suggestedPrefix.replaceAll("[^a-zA-Z0-9_]", " ").trim();
        final int spaceIndex = identifiersOnlyPrefix.indexOf(" ");
        final String prefix = spaceIndex > 0
                ? identifiersOnlyPrefix.substring(0, spaceIndex)
                : identifiersOnlyPrefix;
        final String shortPrefix =
                (prefix.length() < MAX_PREFIX_LENGTH
                        ? prefix
                        : prefix.substring(0, MAX_PREFIX_LENGTH)).toUpperCase();
        int i = 0;
        String name = shortPrefix + i;
        while (used.contains(name)) {
            name = shortPrefix + (++i);

        }
        used.add(name);
        return name;
    }

    /**
     * Force the "generated" name to be exactly as suggested name.
     * It responsibility of the caller to supply identifier.
     * @param name suggestion.
     */
    public final void force(final String name) {
        Bug.reportIf(used.contains(name));
        used.add(name);
    }
}
