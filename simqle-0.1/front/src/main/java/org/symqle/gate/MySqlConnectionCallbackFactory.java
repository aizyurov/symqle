package org.symqle.gate;

/**
 * @author lvovich
 */
public class MySqlConnectionCallbackFactory implements ConnectionCallbackFactory {

    @Override
    public String getName() {
        return "MySQL";
    }

    @Override
    public ConnectionCallback createCallback() {
        return new MySqlConnectionCallback();
    }
}
