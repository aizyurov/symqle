package org.symqle.util;

/**
 * A reference, which can create a referent on demand.
 * Referent can be null.
 * @param <T> referent type
 */
public abstract class OnDemand<T> {
    private T referent;
    private boolean initialized;

    /**
     * Returns the referent.
     * The referent is created at the first call to this method.
     * @return the referent (may be null if {@link #init} returns null)
     */
    public final T get() {
        if (!initialized) {
            referent = init();
            initialized = true;
        }
        return referent;
    }

    /**
     * Creates the referent.
     * Subclasses must implement this method.
     * @return the created object
     */
    protected abstract T init();
}
