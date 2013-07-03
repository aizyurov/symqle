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


    /**
     * Sets an object to this context.
     * The clazz parameter is a key.
     * If the context already contains this key, the value is overwritten.
     * @param clazz the key
     * @param impl the value to store
     * @param <T> type of the value
     */
    public final <T> void set(final Class<T> clazz, final T impl) {
        theContext.put(clazz, impl);
    }

    /**
     * Gets an object from the context by key
     * @param clazz the key
     * @param <T> type of the returned value
     * @return the object stored under this key; null if not found
     */
    @SuppressWarnings("unchecked")
    public final <T> T get(final Class<T> clazz) {
        return (T) theContext.get(clazz);
    }

    private final Map<Class<?>, Object> theContext = new HashMap<Class<?>, Object>();


}
