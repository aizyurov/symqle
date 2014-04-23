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
import org.symqle.sql.GenericDialect;

import static org.symqle.querybuilder.SqlTerm.ALL;
import static org.symqle.querybuilder.SqlTerm.DISTINCT;
import static org.symqle.querybuilder.SqlTerm.EXCEPT;
import static org.symqle.querybuilder.SqlTerm.UNION;
import static org.symqle.querybuilder.SqlTerm.LEFT_PAREN;
import static org.symqle.querybuilder.SqlTerm.RIGHT_PAREN;


/**
 * Dialect of <a href="http://www.h2database.com/html/main.html">H2 Database Engine</a>.
 * @author lvovich
 */
public class H2Dialect extends GenericDialect {

    @Override
    public String getName() {
        return "H2";
    }

    @Override
    public String fallbackTableName() {
        return "(VALUES(1))";
    }

    @Override
    public SqlBuilder QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_EXCEPT_ALL_QueryTerm(
            final SqlBuilder qe, final SqlBuilder other) {
        // H2 requires additional parentheses; use if other does not start with parenthesis
        if (other.firstChar() != '(') {
            return concat(qe, EXCEPT, ALL, LEFT_PAREN, other, RIGHT_PAREN);
        } else {
            return super.QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_EXCEPT_ALL_QueryTerm(qe, other);
        }
    }

    @Override
    public SqlBuilder QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_EXCEPT_DISTINCT_QueryTerm(
            final SqlBuilder qe, final SqlBuilder other) {
        // H2 requires additional parentheses; use if other does not start with parenthesis
        if (other.firstChar() != '(') {
            return concat(qe, EXCEPT, DISTINCT, LEFT_PAREN, other, RIGHT_PAREN);
        } else {
            return super.QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_EXCEPT_DISTINCT_QueryTerm(qe, other);
        }
    }

    @Override
    public SqlBuilder QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_EXCEPT_QueryTerm(
            final SqlBuilder qe, final SqlBuilder other) {
        // H2 requires additional parentheses; use if other does not start with parenthesis
        if (other.firstChar() != '(') {
            return concat(qe, EXCEPT, LEFT_PAREN, other, RIGHT_PAREN);
        } else {
            return super.QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_EXCEPT_QueryTerm(qe, other);
        }
    }

    @Override
    public SqlBuilder QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_UNION_ALL_QueryTerm(
            final SqlBuilder qe, final SqlBuilder other) {
        // H2 requires additional parentheses; use if other does not start with parenthesis
        if (other.firstChar() != '(') {
            return concat(qe, UNION, ALL, LEFT_PAREN, other, RIGHT_PAREN);
        } else {
            return super.QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_UNION_ALL_QueryTerm(qe, other);
        }
    }

    @Override
    public SqlBuilder QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_UNION_DISTINCT_QueryTerm(
            final SqlBuilder qe, final SqlBuilder other) {
        // H2 requires additional parentheses; use if other does not start with parenthesis
        if (other.firstChar() != '(') {
            return concat(qe, UNION, DISTINCT, LEFT_PAREN, other, RIGHT_PAREN);
        } else {
            return super.QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_UNION_DISTINCT_QueryTerm(qe, other);
        }
    }

    @Override
    public SqlBuilder QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_UNION_QueryTerm(
            final SqlBuilder qe, final SqlBuilder other) {
        // H2 requires additional parentheses; use if other does not start with parenthesis
        if (other.firstChar() != '(') {
            return concat(qe, UNION, LEFT_PAREN, other, RIGHT_PAREN);
        } else {
            return super.QueryExpressionBodyScalar_is_QueryExpressionBodyScalar_UNION_QueryTerm(qe, other);
        }
    }
}
