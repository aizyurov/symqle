package org.simqle.jdbc;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 01.07.2013
 * Time: 11:12:56
 * To change this template use File | Settings | File Templates.
 */
public class DebugConfiguration implements Configuration {
    @Override
    public boolean allowNoFrom() {
        return true;
    }

    @Override
    public boolean allowImplicitCrossJoins() {
        return true;
    }    
}
