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
 * Trivial mutable implementation of {@link Configuration}.
 * Default settings are {@code false}
 *
 */
public class UpdatableConfiguration implements Configuration {
    boolean noFromOk = false;
    boolean implicitCrossJoinsOk = false;

    /**
     * Sets allowNoFrom.
     * @param noFromOk true to allow
     */
    public void setNoFromOk(final boolean noFromOk) {
        this.noFromOk = noFromOk;
    }

    /**
     * Sets allowImplicitCrossJoins
     * @param implicitCrossJoinsOk  true to allow
     */
    public void setImplicitCrossJoinsOk(final boolean implicitCrossJoinsOk) {
        this.implicitCrossJoinsOk = implicitCrossJoinsOk;
    }

    @Override
    public boolean allowImplicitCrossJoins() {
        return implicitCrossJoinsOk;
    }

    @Override
    public boolean allowNoFrom() {
        return noFromOk;
    }

}
