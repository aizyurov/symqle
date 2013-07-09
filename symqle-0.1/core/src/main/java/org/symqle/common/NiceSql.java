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

import java.util.regex.Pattern;

/**
 * Defines nice formatting of its text, removing unnecessary whitespace.
 * It is recommended that all implementations of Sql extends this class.
 */
public abstract class NiceSql implements Sql {

    @Override
    public String toString() {
        final String s0 = REPEATING_WHITESPACE.matcher(sql()).replaceAll(" ");
        final String s1 = UNNEEDED_SPACE_AFTER.matcher(s0).replaceAll("");
        final String s2 = UNNEEDED_SPACE_BEFORE.matcher(s1).replaceAll("");
        return s2.trim();
    }

    private final static Pattern REPEATING_WHITESPACE = Pattern.compile("\\s+");
    private final static Pattern UNNEEDED_SPACE_AFTER = Pattern.compile("(?<=[(.]) ");
    private final static Pattern UNNEEDED_SPACE_BEFORE = Pattern.compile(" (?=[().,])");
}
