package org.symqle.common;

/**
 * @author lvovich
 */
public class FormattingRules {

    public static boolean needSpaceBetween(char first, char second) {

        switch(first) {
            case '(' :
            case '.' :
                return false;
            default:
                switch (second) {
                    case ')' :
                    case '(' :
                    case '.' :
                    case ',' :
                        return false;
                    default:
                        return true;
                }
        }
    }
}
