package org.simqle;

/**
 * @author lvovich
 */
public abstract class PrefixedNameProvider implements UniqueNameProvider {

    private int counter = 0;
    private final String prefix;

    protected PrefixedNameProvider(final String prefix) {
        this.prefix = prefix;
    }

    @Override
    public final String getUniqueName() {
        return prefix + (++counter);
    }
}
