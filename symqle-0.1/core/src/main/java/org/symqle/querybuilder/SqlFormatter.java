package org.symqle.querybuilder;

import org.symqle.common.Sql;

/**
 * @author lvovich
 */
public class SqlFormatter {

    private SqlFormatter() {
    }

    static {
        new SqlFormatter();
    }

    public static String formatText(Sql input) {
        StringBuilder source = new StringBuilder();
        input.appendTo(source);
        final int length = source.length();
        char[] buffer = new char[length+1];
        char current = ' ';
        int offset = 0;
        int i;
        for (i=0; i < length; i++) {
            char next = source.charAt(i);
            switch (current) {
                case ' ':
                    switch(next) {
                        case ' ':
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
                        case ' ':
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
        buffer[offset++] = current;
        int start = offset > 0 && buffer[0] == ' ' ? 1 : 0;
        int newLength = offset - start;
        return new String(buffer, start, newLength);
    }

}
