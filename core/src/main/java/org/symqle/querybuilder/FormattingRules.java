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

package org.symqle.querybuilder;

/**
 * Rules for SQL statements formatting. Currently only one rule is defined.
 * @author lvovich
 */
public final class FormattingRules {

    private FormattingRules() {
    }

    static {
        new FormattingRules();
    }

    /**
     * Separator to insert between SQL terminal symbols.
     * Space is inserted between 2 non-punctuation symbols to separate them.
     * Additionall, space is inserted sometimes to improve readability, e.g.
     * after comma and after right parenthesis, after +,- etc.
     * Not inserted before and after period, before and after left parenthesis.
     * @param first left character
     * @param second right character
     * @return separator, may be empty string.
     */
    public static String separator(final char first, final char second) {

        switch(first) {
            case '(' :
            case '.' :
                return "";
            default:
                switch (second) {
                    case ')' :
                    case '(' :
                    case '.' :
                    case ',' :
                        return "";
                    default:
                        return " ";
                }
        }
    }
}
