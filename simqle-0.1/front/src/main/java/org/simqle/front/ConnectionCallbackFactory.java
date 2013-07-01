package org.simqle.front;

/**
* @author lvovich
*/
public interface ConnectionCallbackFactory {
    public String getName();
    public ConnectionCallback createCallback();
}
