package org.simqle.front;

/**
 * @author lvovich
 */
public class MySqlConnectionCallbackFactory implements ConnectionCallbackFactory {

    @Override
    public String getName() {
        return "MySql";
    }

    @Override
    public ConnectionCallback createCallback() {
        return new MySqlConnectionCallback();
    }
}
