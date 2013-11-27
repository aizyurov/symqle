package org.symqle.dialect;

import org.symqle.sql.GenericDialect;

/**
 * @author lvovich
 */
public class FastGenericDialect extends GenericDialect {

    @Override
    public String formatSql(final String source) {
        final int length = source.length();
        char[] buffer = new char[length];
        char current = ' ';
        int offset = 0;
        int i;
        for (i=0; i<length; i++) {
            char next = source.charAt(i);
            switch (current) {
                case ' ': case '\t': case '\r': case '\n':
                    switch(next) {
                        case ' ': case '\t': case '\r': case '\n':
                        case '(': case ')' : case '.' :case ',':
                            current = next;
                            break;
                        default:
                            buffer[offset++] = current;
                            current = next;
                    }
                    break;
                case '(' :case '.' :
                    switch(next) {
                        case ' ': case '\t': case '\r': case '\n':
                            break;
                        default:
                            buffer[offset++] = current;
                            current = next;
                    }
                    break;
                default:
                    buffer[offset++] = current;
                    current = next;
            }
        }
        int start = offset > 0 && buffer[0] == ' ' ? 1 : 0;
        int newLength = offset - start;
        return new String(buffer, start, newLength);
    }
}
