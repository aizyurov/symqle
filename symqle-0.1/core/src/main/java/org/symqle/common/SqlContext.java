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


import java.util.HashMap;
import java.util.Map;

/**
 * A context used by Symqle query builder.
 * It is like a map, key is Class, value is object of this class.
 */
public class SqlContext {

    private SqlContext(final Map<Class<?>, Object> source) {
        theContext.putAll(source);
    }
    /**
     * Gets an object from the context by key.
     * @param clazz the key
     * @param <T> type of the returned value
     * @return the object stored under this key; null if not found
     */
    @SuppressWarnings("unchecked")
    public final <T> T get(final Class<T> clazz) {
        return (T) theContext.get(clazz);
    }

//    /**
//     * Creates a copy of {@code this} with one lkey/value pair replaced or added.
//     * @param clazz
//     * @param impl
//     * @param <T>
//     * @return
//     */
//    public final <T> SqlContext put(final Class<T> clazz, final T impl) {
//        final SqlContext newContext = new SqlContext();
//        newContext.theContext.putAll(theContext);
//        newContext.theContext.put(clazz, impl);
//        return newContext;
//    }

    public final Builder newBuilder() {
        return new Builder(this.theContext);
    }

    private final Map<Class<?>, Object> theContext = new HashMap<Class<?>, Object>();

    public static class Builder {

        private final Map<Class<?>, Object> theContext = new HashMap<Class<?>, Object>();

        public Builder() {
        }

        private Builder(final Map<Class<?>, Object> source) {
            this.theContext.putAll(source);
        }

        public <T> Builder put(final Class<T> clazz, final T impl) {
            theContext.put(clazz, impl);
            return this;
        }

        public SqlContext toSqlContext() {
            return new SqlContext(this.theContext);
        }
    }


}
