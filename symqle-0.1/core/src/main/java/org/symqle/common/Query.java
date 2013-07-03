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
 * This interface extends both Sql and RowMapper<JavaType>.
 * Instances of this interface are associated with Sql syntax elements, which can appear in
 * SELECT clause. These elements provide Query, which can construct objects of JavaType class
 * from the values returned in result set. RowMapper part should be in sync with Sql one:
 * it accesses elements by labels (or positions) defined in the sql.
 * @author Alexander Izyurov
 * @version 0.1
 * @param <JavaType> the type of associated Java objects
 */
public interface Query<JavaType> extends Sql, RowMapper<JavaType> {
}
