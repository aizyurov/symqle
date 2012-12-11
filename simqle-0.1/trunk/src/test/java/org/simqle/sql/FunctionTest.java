package org.simqle.sql;

import org.simqle.Element;

import java.sql.SQLException;

/**
 * @author lvovich
 */
public class FunctionTest {

    public static class Concat extends FunctionCall<String> {

        public Concat() {
            super("concat");
        }

        public RoutineInvocation<String> apply(final ValueExpression<?> arg1, final ValueExpression<?> arg2) {
            return super.apply(arg1, arg2);
        }

        @Override
        public String value(final Element element) throws SQLException {
            return element.getString();
        }
    }
}
