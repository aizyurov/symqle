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

package org.symqle.common;

import java.sql.SQLException;

/**
 * Represents an SqlBuilder element composed from a list of sub-elements.
 * Provides implementation of {@link #appendTo(StringBuilder)} ()} and {@link #setParameters(SqlParameters)}
 * @author Alexander Izyurov
 */
public class CompositeSqlBuilder implements SqlBuilder {
    private final SqlBuilder first;
    private final SqlBuilder[] other;

    /**
     * Constructs composite SqlBuilder from elements.
     * @param first the first element of sequence, not null
     * @param other elements, optional (but each not null)
     */
    public CompositeSqlBuilder(final SqlBuilder first, final SqlBuilder... other) {
        this.first = first;
        this.other = other;
    }

    @Override
    public void appendTo(final StringBuilder builder) {
        first.appendTo(builder);
        for (SqlBuilder element: other) {
            final char lastChar = builder.charAt(builder.length() - 1);
            final char nextChar = element.firstChar();
//            if (lastChar != '(' &&
//                    lastChar != '.' &&
//                    nextChar != ')' &&
//                    nextChar != '(' &&
//                    nextChar != '.' &&
//                    nextChar != ',') {
//                builder.append(' ');
//            }
//            element.appendTo(builder);
            if (FormattingRules.needSpaceBetween(lastChar, nextChar)) {
                builder.append(' ');
            }
            element.appendTo(builder);
        }
    }

    /**
     * Sets OutBox by delegation to each member in turn.
     * @param p OutBox interface to write parameter values into
     */
    public final void setParameters(final SqlParameters p) throws SQLException {
        first.setParameters(p);
        for (SqlBuilder element : this.other) {
            element.setParameters(p);
        }
    }

    @Override
    public char firstChar() {
        return first.firstChar();
    }
}
