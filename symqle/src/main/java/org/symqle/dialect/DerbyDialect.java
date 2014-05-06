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

package org.symqle.dialect;

import org.symqle.common.SqlBuilder;
import org.symqle.common.StringSqlBuilder;
import org.symqle.sql.GenericDialect;

import static org.symqle.querybuilder.SqlTerm.*;


/**
 * Dialect for <a href="http://db.apache.org/derby/">Apache Derby database</a>.
 * @author lvovich
 */
public class DerbyDialect extends GenericDialect {

    @Override
    public String getName() {
        return "Apache Derby";
    }

    @Override
    public String fallbackTableName() {
        return "(VALUES(1))";
    }

    @Override
    public SqlBuilder BooleanPrimary_is_ValueExpressionPrimary(final SqlBuilder e) {
        // Derby does not cast implicitly; using explicit case
        return concat(CAST, LEFT_PAREN, e, AS, BOOLEAN, RIGHT_PAREN);
    }

    @Override
    public SqlBuilder ValueExpression_is_BooleanExpression(final SqlBuilder bve) {
        // derby dialect misunderstands usage of BooleanExpression where ValueExpression is required;
        // surrounding with parentheses to avoid it
        return concat(LEFT_PAREN, bve, RIGHT_PAREN);
    }

    @Override
    public SqlBuilder
    StringExpression_is_SUBSTRING_LEFT_PAREN_StringExpression_FROM_NumericExpression_FOR_NumericExpression_RIGHT_PAREN(
            final SqlBuilder s, final SqlBuilder start, final SqlBuilder len) {
        return concat(new StringSqlBuilder("SUBSTR"), LEFT_PAREN, s, COMMA, start, COMMA, len, RIGHT_PAREN);
    }

    @Override
    public SqlBuilder
    StringExpression_is_SUBSTRING_LEFT_PAREN_StringExpression_FROM_NumericExpression_RIGHT_PAREN(
            final SqlBuilder s, final SqlBuilder start) {
        return concat(new StringSqlBuilder("SUBSTR"), LEFT_PAREN, s, COMMA, start, RIGHT_PAREN);
    }

    @Override
    public SqlBuilder
    NumericExpression_is_POSITION_LEFT_PAREN_StringExpression_IN_StringExpression_RIGHT_PAREN(
            final SqlBuilder pattern, final SqlBuilder source) {
        return concat(new StringSqlBuilder("LOCATE"), LEFT_PAREN, pattern, COMMA, source, RIGHT_PAREN);
    }

    @Override
    public SqlBuilder NumericExpression_is_CHAR_LENGTH_LEFT_PAREN_StringExpression_RIGHT_PAREN(
            final SqlBuilder string) {
        return concat(LENGTH, LEFT_PAREN, string, RIGHT_PAREN);
    }
}
