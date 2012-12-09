package org.simqle;

import org.simqle.ColumnNameProvider;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 07.11.11
 * Time: 17:10
 * To change this template use File | Settings | File Templates.
 */
public class NullColumnNameProvider extends ColumnNameProvider {
    @Override
    public String getUniqueName() {
        return null;
    }
}
