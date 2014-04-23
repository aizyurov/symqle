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
import org.symqle.common.Mappers;

/**
 * These functions are supported by most dialects.
 * @author lvovich
 */
public final class Functions {

    private Functions() {
    }

    static {
        new Functions();
    }

    /**
     * Absolute value.
     * @param e function argument
     * @param <T> associated Java type
     * @return object representing {@code ABS(e)}
     */
    public static <T> AbstractRoutineInvocation<T> abs(final ValueExpression<T> e) {
        return SqlFunction.create("ABS", e.getMapper()).apply(e);
    }

    /**
     * Integer division remainder.
     * @param dividend any ValueExpression
     * @param divisor  any ValueExpression
     * @return object representing {@code MOD(dividend, divisor)}
     */
    public static AbstractRoutineInvocation<Number> mod(
            final ValueExpression<?> dividend, final ValueExpression<?> divisor) {
        return SqlFunction.create("MOD", CoreMappers.NUMBER).apply(dividend,  divisor);
    }

    /**
     * Natural logarithm.
     * @param arg the argument
     * @return object representing {@code LN(arg)}
     */
    public static AbstractRoutineInvocation<Number> ln(
            final ValueExpression<?> arg) {
        return SqlFunction.create("LN", CoreMappers.NUMBER).apply(arg);
    }

    /**
     * Exponent.
     * @param arg the argument
     * @return object representing {@code EXP(arg)}
     */
    public static AbstractRoutineInvocation<Number> exp(
            final ValueExpression<?> arg) {
        return SqlFunction.create("EXP", CoreMappers.NUMBER).apply(arg);
    }

    /**
     * Suqare root.
     * @param arg the argument
     * @return object representing {@code SQRT(arg)}
     */
    public static AbstractRoutineInvocation<Number> sqrt(
            final ValueExpression<?> arg) {
        return SqlFunction.create("SQRT", CoreMappers.NUMBER).apply(arg);
    }

    /**
     * Round to nearest less or equal integer.
     * @param arg the argument
     * @param <T> associated Java type
     * @return object representing {@code FLOOR(arg)}
     */
    public static <T> AbstractRoutineInvocation<T> floor(
            final ValueExpression<T> arg) {
        return SqlFunction.create("FLOOR", arg.getMapper()).apply(arg);
    }

    /**
     * Round to nearest greater or equal integer.
     * @param arg the argument
     * @param <T> associated Java type
     * @return object representing {@code CEIL(arg)}
     */
    public static <T> AbstractRoutineInvocation<T> ceil(
            final ValueExpression<T> arg) {
        return SqlFunction.create("CEIL", arg.getMapper()).apply(arg);
    }


    /**
     * Power.
     * @param base the argument
     * @param exponent the exponent
     * @return object representing {@code POWER(base, exponent)}
     */
    public static AbstractRoutineInvocation<Number> power(
            final ValueExpression<?> base, final ValueExpression<?> exponent) {
        return SqlFunction.create("POWER", CoreMappers.NUMBER).apply(base,  exponent);
    }

    /**
     * Power.
     * @param base the argument
     * @param exponent the exponent
     * @return object representing {@code POWER(base, ?)} with value of parameter set to exponent.
     */
    public static AbstractRoutineInvocation<Number> power(
            final ValueExpression<?> base, final Number exponent) {
        return power(base, DynamicParameter.create(CoreMappers.NUMBER, exponent));
    }

    /**
     * Converts character type expression to upper case.
     * @param arg the argument
     * @return object representing {@code UPPER(arg)}
     */
    public static AbstractRoutineInvocation<String> toUpper(
            final ValueExpression<String> arg) {
        return SqlFunction.create("UPPER", Mappers.STRING).apply(arg);
    }

    /**
     * Converts character type expression to lower case.
     * @param arg the argument
     * @return object representing {@code LOWER(arg)}
     */
    public static AbstractRoutineInvocation<String> toLower(
            final ValueExpression<String> arg) {
        return SqlFunction.create("LOWER", Mappers.STRING).apply(arg);
    }

}
