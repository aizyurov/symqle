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

package org.symqle.jdbc;

/**
 * Defines query builder behavior..
 *
 */
public interface Configuration {

    /**
     * Defines whether FROM with no tables is allowed.
     * If this option is set, query builder will either skip FROM clause
     * (if it is allowed by current dialect) or add single row -single column table to
     * FROM clause (like "FROM dual" in Oracle") if there are no tables to construct FROM clause.
     * If this method returns false, no such attempt will be made and query builder will throw an exception
     * if there are no tables for FROM clause.
     * @return true of no tables for FROM clause is OK
     */
    boolean allowNoFrom();

    /**
     * Defines whether implicit cross joins are allowed,
     * Query builder will throw an exception if this method returns false
     * and implicit cross join is present.
     * @return true if implicit cross joins are OK.
     */
    boolean allowImplicitCrossJoins();
}
