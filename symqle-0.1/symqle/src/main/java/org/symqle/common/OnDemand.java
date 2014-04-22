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
 * A reference, which can create a referent on demand.
 * Referent can be null.
 * @param <T> referent type
 */
public abstract class OnDemand<T> {
    private T referent;
    private boolean initialized;

    /**
     * Returns the referent.
     * The referent is created at the first call to this method.
     * @return the referent (may be null if {@link #construct} returns null)
     */
    public final T get() {
        if (!initialized) {
            referent = construct();
            initialized = true;
        }
        return referent;
    }

    /**
     * Creates the referent.
     * Subclasses must implement this method.
     * @return the created object
     */
    protected abstract T construct();
}
