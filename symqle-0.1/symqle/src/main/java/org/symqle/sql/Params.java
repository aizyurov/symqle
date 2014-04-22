/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

package org.symqle.sql;

import org.symqle.common.CoreMappers;

/**
 * A utility class with shortcuts to DynamicParameter factory methods.
 * @author lvovich
 */
public class Params {

    private Params() {
    }

    static {
        new Params();
    }

    /**
     * Creates a Boolean parameter.
     * @param b initial value
     * @return created DynamicParameter
     */
    public static DynamicParameter<Boolean> p(boolean b) {
        return DynamicParameter.create(CoreMappers.BOOLEAN, b);
    }

    /**
     * Creates an Integer parameter.
     * @param x initial value
     * @return created DynamicParameter
     */
    public static DynamicParameter<Integer> p(int x) {
        return DynamicParameter.create(CoreMappers.INTEGER, x);
    }

    /**
     * Creates a Long parameter.
     * @param x initial value
     * @return created DynamicParameter
     */
    public static DynamicParameter<Long> p(long x) {
        return DynamicParameter.create(CoreMappers.LONG, x);
    }

    /**
     * Creates a String parameter.
     * @param x initial value
     * @return created DynamicParameter
     */
    public static DynamicParameter<String> p(String x) {
        return DynamicParameter.create(CoreMappers.STRING, x);
    }

    /**
     * Creates a Double parameter.
     * @param x initial value
     * @return created DynamicParameter
     */
    public static DynamicParameter<Double> p(double x) {
        return DynamicParameter.create(CoreMappers.DOUBLE, x);
    }

}
