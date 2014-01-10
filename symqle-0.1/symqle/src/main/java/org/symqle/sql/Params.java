package org.symqle.sql;

import org.symqle.common.Mappers;

/**
 * @author lvovich
 */
public class Params {

    private Params() {
    }

    static {
        new Params();
    }

    public static DynamicParameter<Boolean> p(boolean b) {
        return DynamicParameter.create(Mappers.BOOLEAN, b);
    }

    public static DynamicParameter<Integer> p(int x) {
        return DynamicParameter.create(Mappers.INTEGER, x);
    }

    public static DynamicParameter<Long> p(long x) {
        return DynamicParameter.create(Mappers.LONG, x);
    }

    public static DynamicParameter<String> p(String x) {
        return DynamicParameter.create(Mappers.STRING, x);
    }

    public static DynamicParameter<Double> p(double x) {
        return DynamicParameter.create(Mappers.DOUBLE, x);
    }

}
