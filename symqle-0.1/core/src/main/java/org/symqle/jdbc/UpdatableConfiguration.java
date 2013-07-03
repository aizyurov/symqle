package org.symqle.jdbc;

/**
 * Trivial mutable implementation of {@link Configuration}.
 * Default settings are {@code false}
 *
 */
public class UpdatableConfiguration implements Configuration {
    boolean noFromOk = false;
    boolean implicitCrossJoinsOk = false;

    /**
     * Sets allowNoFrom.
     * @param noFromOk true to allow
     */
    public void setNoFromOk(final boolean noFromOk) {
        this.noFromOk = noFromOk;
    }

    /**
     * Sets allowImplicitCrossJoins
     * @param implicitCrossJoinsOk  true to allow
     */
    public void setImplicitCrossJoinsOk(final boolean implicitCrossJoinsOk) {
        this.implicitCrossJoinsOk = implicitCrossJoinsOk;
    }

    @Override
    public boolean allowImplicitCrossJoins() {
        return implicitCrossJoinsOk;
    }

    @Override
    public boolean allowNoFrom() {
        return noFromOk;
    }

}
