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
 * Mapper can extract a value from InBox as Java object and
 * set a value of the same type to an OutBox.
 * @author lvovich
 * @param <T> the type of extracted value
 */
public interface Mapper<T> {
    /**
     * Extracts value from an InBox.
     * @param inBox the inBox to extract value from
     * @return the value
     * @throws SQLException the value is not convertible to type T.
     */
    T value(InBox inBox) throws SQLException;

    /**
     * Sets give value to an OutBox.
     * @param param the parameter to set value to.
     * @param value the value to set
     * @throws SQLException the value of type T cannot be set to this parameter.
     */
    void setValue(OutBox param, T value) throws SQLException;
}
