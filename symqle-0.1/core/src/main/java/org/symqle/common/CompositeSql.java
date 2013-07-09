package org.symqle.common;

import java.sql.SQLException;

/**
 * Represents an Sql element composed from a list of sub-elements.
 * Provides implementation of {@link #getSqlText()} and {@link #setParameters(SqlParameters)}
 * @author Alexander Izyurov
 */
public class CompositeSql extends NiceSql {
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

    /**
     * Constructs Sql text as concatenation of Sql text of elements.
     * @return constructed text
     */
    public final String getSqlText() {
        // minor optimization
        if (other.length==0) {
            return first.getSqlText();
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append(first.getSqlText());
            for (Sql element : this.other) {
                builder.append(' ');
                builder.append(element.getSqlText());
            }
            return builder.toString();
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
