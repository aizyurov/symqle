package org.symqle.generic;

import org.symqle.common.CompositeSql;
import org.symqle.common.Mapper;
import org.symqle.common.Mappers;
import org.symqle.common.Sql;
import org.symqle.common.SqlContext;
import org.symqle.querybuilder.SqlTerm;
import org.symqle.sql.AbstractRoutineInvocation;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.SqlFunction;
import org.symqle.sql.StringExpression;
import org.symqle.sql.ValueExpression;

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

    public static AbstractRoutineInvocation<Number> power(
            final ValueExpression<?> base, final Number exponent) {
        return power(base, DynamicParameter.create(Mappers.NUMBER, exponent));
    }

    public static AbstractRoutineInvocation<Integer> position(final StringExpression<?> pattern, final StringExpression<?> string) {
        return new AbstractRoutineInvocation() {
            @Override
            public Mapper getMapper() {
                return Mappers.INTEGER;
            }

            @Override
            public Sql z$sqlOfRoutineInvocation(final SqlContext context) {
                return new CompositeSql(SqlTerm.POSITION, SqlTerm.LEFT_PAREN, pattern.z$sqlOfStringExpression(context),
                        SqlTerm.IN, string.z$sqlOfStringExpression(context), SqlTerm.RIGHT_PAREN);
            }
        };
    }

    public static AbstractRoutineInvocation<Integer> position(final String pattern, final StringExpression<?> string) {
        return position(Params.p(pattern), string);
    }




}
