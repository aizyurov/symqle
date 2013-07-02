package org.symqle.gate;

/**
* @author lvovich
*/
public interface ConnectionCallbackFactory {
    public String getName();
    public ConnectionCallback createCallback();
}
