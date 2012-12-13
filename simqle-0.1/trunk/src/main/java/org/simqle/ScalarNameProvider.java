package org.simqle;

/**
 * @author lvovich
 */
public class ScalarNameProvider implements UniqueNameProvider {

    boolean neverCalled = true;

    @Override
    public String getUniqueName() {
        if (neverCalled) {
            return "S0";
        } else {
            throw new IllegalStateException(getClass().getSimpleName() + " may be called once only");
        }
    }
}
