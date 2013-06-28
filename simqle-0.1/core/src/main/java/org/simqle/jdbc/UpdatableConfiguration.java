package org.simqle.jdbc;

/**
* Base implementation of Configuration
*
*/
public class UpdatableConfiguration implements Configuration {
    boolean noFromOk = false;
    boolean implicitCrossJoinsOk = false;

    public void setNoFromOk(final boolean noFromOk) {
        this.noFromOk = noFromOk;
    }

    public void setImplicitCrossJoinsOk(final boolean implicitCrossJoinsOk) {
        this.implicitCrossJoinsOk = implicitCrossJoinsOk;
    }

    public static UpdatableConfiguration copyOf(final Configuration configuration) {
        final UpdatableConfiguration copy = new UpdatableConfiguration();
        copy.setNoFromOk(configuration.allowNoFrom());
        copy.setImplicitCrossJoinsOk(configuration.allowImplicitCrossJoins());
        return copy;
    }

    /**
    * Defines whether implicit cross joins are allowed,
    */
    public boolean allowImplicitCrossJoins() {
        return implicitCrossJoinsOk;
    }

    /**
    * Defines whether empty FROM is allowed.
    * @return true of no FROM clause is OK
    */
    public boolean allowNoFrom() {
        return noFromOk;
    }

}
