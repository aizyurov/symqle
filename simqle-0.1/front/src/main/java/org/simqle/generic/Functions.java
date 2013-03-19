package org.simqle.generic;

import org.simqle.Mappers;
import org.simqle.sql.AbstractRoutineInvocation;
import org.simqle.sql.SqlFunction;
import org.simqle.sql.ValueExpression;

/**
 * These functions are supported by most dialects
 * @author lvovich
 */
public class Functions {

    private Functions() {
    }

    static {
        new Functions();

    }

    public static <T> AbstractRoutineInvocation<T> abs(final ValueExpression<T> e) {
        return SqlFunction.create("ABS", e.getMapper()).apply(e);
    }

    public static AbstractRoutineInvocation<Number> mod(
            final ValueExpression<?> dividend, final ValueExpression<?> divisor) {
        return SqlFunction.create("MOD", Mappers.NUMBER).apply(dividend,  divisor);
    }

    public static AbstractRoutineInvocation<Number> ln(
            final ValueExpression<?> arg) {
        return SqlFunction.create("LN", Mappers.NUMBER).apply(arg);
    }

    public static AbstractRoutineInvocation<Number> exp(
            final ValueExpression<?> arg) {
        return SqlFunction.create("EXP", Mappers.NUMBER).apply(arg);
    }

    public static AbstractRoutineInvocation<Number> sqrt(
            final ValueExpression<?> arg) {
        return SqlFunction.create("SQRT", Mappers.NUMBER).apply(arg);
    }

    public static <T> AbstractRoutineInvocation<T> floor(
            final ValueExpression<T> arg) {
        return SqlFunction.create("FLOOR", arg.getMapper()).apply(arg);
    }

    public static <T> AbstractRoutineInvocation<T> ceil(
            final ValueExpression<T> arg) {
        return SqlFunction.create("CEIL", arg.getMapper()).apply(arg);
    }

    public static AbstractRoutineInvocation<Number> power(
            final ValueExpression<?> base, final ValueExpression<?> exponent) {
        return SqlFunction.create("POWER", Mappers.NUMBER).apply(base,  exponent);
    }







}
