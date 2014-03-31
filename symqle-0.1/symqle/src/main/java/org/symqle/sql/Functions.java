package org.symqle.sql;

import org.symqle.common.CoreMappers;
import org.symqle.querybuilder.SqlTerm;

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
        return SqlFunction.create("MOD", CoreMappers.NUMBER).apply(dividend,  divisor);
    }

    public static AbstractRoutineInvocation<Number> ln(
            final ValueExpression<?> arg) {
        return SqlFunction.create("LN", CoreMappers.NUMBER).apply(arg);
    }

    public static AbstractRoutineInvocation<Number> exp(
            final ValueExpression<?> arg) {
        return SqlFunction.create("EXP", CoreMappers.NUMBER).apply(arg);
    }

    public static AbstractRoutineInvocation<Number> sqrt(
            final ValueExpression<?> arg) {
        return SqlFunction.create("SQRT", CoreMappers.NUMBER).apply(arg);
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
        return SqlFunction.create("POWER", CoreMappers.NUMBER).apply(base,  exponent);
    }

    public static AbstractRoutineInvocation<Number> power(
            final ValueExpression<?> base, final Number exponent) {
        return power(base, DynamicParameter.create(CoreMappers.NUMBER, exponent));
    }

    public static AbstractRoutineInvocation<Integer> charLength(final ValueExpression<?> expression) {
        return SqlFunction.create(SqlTerm.CHAR_LENGTH.toString(), CoreMappers.INTEGER).apply(expression);
    }

}
