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

/**
 * Sql has text and also can set parameters.
 * Implementation of Parameterizer should be consistent with text:
 * number of calls to parameters.next() must be the same as the number of
 * parameter placeholders in the text.
 * @author lvovich
 */
public interface Sql extends Parameterizer {

    /**
     * The SQL text.
     * @return the text
     */
    String text();
}
