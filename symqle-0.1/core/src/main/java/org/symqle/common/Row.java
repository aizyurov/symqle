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

import org.symqle.jdbc.QueryEngine;

/**
 * An abstraction of a single row of a result set.
 * It does not contain methods for scrolling, closing result set etc.,
 * just access to row elements.
 * @author Alexander Izyurov
 */
public interface Row {
    /**
     * Accesses a value slot for a column in the row by label.
     * @param label column label
     * @return the value slot
     */
    InBox getValue(String label);

    /**
     * An engine to execute queries on the same connection.
     * May be used to make query for each row, which depends on row values.
     * Although it is not a good practice, it may be useful.
     * @return proper QueryEngine
     */
    QueryEngine getQueryEngine();

}
