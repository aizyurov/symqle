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
 * Intended for iterative operations on a series of objects.
 * The supplier calls callback once for each object.
 *
 * @param <Arg> type of objects
 * @author Alexander Izyurov
 */
public interface Callback<Arg> {

    /**
     * Called by supplier. The supplier passes arg objects until
     * this method returns false or there are no more object to process.
     * @param arg the object to process.
     * @return true to continue iterations, false to break.
     * @throws SQLException something wrong when processing arg
     */
    boolean iterate(Arg arg) throws SQLException;

}
