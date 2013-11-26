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
 * Represents an Sql element composed from a list of sub-elements.
 * Provides implementation of {@link #sql()} and {@link #setParameters(SqlParameters)}
 * @author Alexander Izyurov
 */
public class CompositeSql implements Sql {
    private final Sql first;
    private final Sql[] other;

    /**
     * Constructs composite Sql from elements.
     * @param first the first element of sequence, not null
     * @param other elements, optional (but each not null)
     */
    public CompositeSql(final Sql first, final Sql... other) {
        this.first = first;
        this.other = other;
    }

    @Override
    public void append(final SqlBuilder builder) {
        first.append(builder);
        for (Sql element: other) {
            builder.append(' ');
            element.append(builder);
        }
    }

    /**
     * Sets SqlParameter by delegation to each member in turn.
     * @param p SqlParameter interface to write parameter values into
     */
    public final void setParameters(final SqlParameters p) throws SQLException {
        first.setParameters(p);
        for (Sql element : this.other) {
            element.setParameters(p);
        }
    }

}
