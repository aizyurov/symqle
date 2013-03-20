package org.simqle.util;

/**
 * @author lvovich
 */
public abstract class LazyRef<T> {
    private T referent;
    private boolean initialized;

    /**
     * Returns the referent.
     * The referent is created at the first call to this method.
     * @return the referent (may be null if {@link #create()} returns null)
     */
    public final T get() {
        if (!initialized) {
            referent = create();
            initialized = true;
        }
        return referent;
    }

    /**
     * Creates the referent.
     * Subclasses must implement this method.
     * @return the created object
     */
    protected abstract T create();
}
