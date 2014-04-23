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

import org.symqle.common.InBox;
import org.symqle.common.Mapper;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A holder of generated keys for insert statements.
 * @author lvovich
 * @param <T> Java type of generated key
 */
public final class GeneratedKeys<T> {

    private final Mapper<T> mapper;
    private final List<T> keys = new ArrayList<T>();

    private GeneratedKeys(final Mapper<T> mapper) {
        this.mapper = mapper;
    }

    /**
     * Creates an instance for given mapper.
     * @param mapper will be used to convert generated kyes to proper Java objects.
     * @param <T> Java type of generated keys.
     * @return new instance
     */
    public static <T> GeneratedKeys<T> create(final Mapper<T> mapper) {
        return new GeneratedKeys<T>(mapper);
    }

    /**
     * Reads next key from InBox.
     * @param inBox the box containing next key
     * @throws SQLException key cannot be converted by mapper.
     */
    void read(final InBox inBox) throws SQLException {
        keys.add(mapper.value(inBox));
    }

    /**
     * All keys collected since construction of {@code this}.
     * @return keys in chronological order.
     */
    public List<T> all() {
        return Collections.unmodifiableList(keys);
    }

    /**
     * A shortcut for all().get(0).
     * @return first generated key
     */
    public T first() {
        return keys.get(0);
    }
}
