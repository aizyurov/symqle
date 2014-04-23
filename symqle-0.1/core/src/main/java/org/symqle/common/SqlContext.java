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
 * A context used by Symqle query builder.
 * It is like a map, key is Class, value is object of this class.
 */
public final class SqlContext {

    private static final int LENGTH = 5;

    private SqlContext(final Class[] keys, final Object[] values) {
        System.arraycopy(keys, 0, this.keys, 0, LENGTH);
        System.arraycopy(values, 0, this.values, 0, LENGTH);
    }
    /**
     * Gets an object from the context by key.
     * @param clazz the key
     * @param <T> type of the returned value
     * @return the object stored under this key; null if not found
     */
    @SuppressWarnings("unchecked")
    public <T> T get(final Class<T> clazz) {
        for (int i = 0; i < LENGTH; i++) {
            if (keys[i] == clazz) {
                return (T) values[i];
            }
        }
        return null;
    }

    /**
     * Creates a Builder, which initiallu contains a copy of (@code this} contents.
     * @return new Builder
     */
    public Builder newBuilder() {
        return new Builder(this.keys, this.values);
    }

    private final Class[] keys = new Class[LENGTH];
    private final Object[] values = new Object[LENGTH];

    /**
     * A builder class for construction of SqlContext.
     */
    public static class Builder {

        private final Class[] keys = new Class[LENGTH];
        private final Object[] values = new Object[LENGTH];

        /**
         * Creates empty Builder.
         */
        public Builder() {
        }

        private Builder(final Class[] keys, final Object[] values) {
            System.arraycopy(keys, 0, this.keys, 0, LENGTH);
            System.arraycopy(values, 0, this.values, 0, LENGTH);
        }

        /**
         * Adds or replaces a value.
         * @param clazz the key
         * @param impl the value to set
         * @param <T> type of clazz
         * @return (@code this}
         */
        public final <T> Builder put(final Class<T> clazz, final T impl) {
            // first replace
            for (int i = 0; i < LENGTH; i++) {
                if (keys[i] == clazz) {
                    values[i] = impl;
                    return this;
                }
            }
            int i = 0;
            // will have ArrayIndexOutOfBoundsException it there is no free slot
            // never expected
            while (keys[i] != null) {
                i++;
            }
            keys[i] = clazz;
            values[i] = impl;
            return this;
        }

        /**
         * Constructs SqlContext containing current contents of (@code this}.
         * @return the constructed SqlContext
         */
        public final SqlContext toSqlContext() {
            return new SqlContext(this.keys, this.values);
        }
    }


}
