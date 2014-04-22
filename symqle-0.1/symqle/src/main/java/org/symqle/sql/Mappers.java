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

package org.symqle.sql;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.Mapper;
import org.symqle.common.OutBox;

import java.sql.SQLException;

/**
 * A collection of Mapper objects.
 * @author lvovich
 */
public class Mappers implements CoreMappers {

    static {
        new Mappers();
    }

    /**
     * Maps to {@link Short}.
     * For use with TINYINT and similar database types.
     */
    public final static Mapper<Short> SHORT = new Mapper<Short>() {
        @Override
        public Short value(final InBox inBox) throws SQLException {
            return inBox.getShort();
        }

        @Override
        public void setValue(final OutBox param, final Short value) throws SQLException {
            param.setShort(value);
        }
    };

    /**
     * Maps to {@link Byte}.
     * For use with TINYINT and similar database types.
     */
    public static final Mapper<Byte> BYTE = new Mapper<Byte>() {
        @Override
        public Byte value(final InBox inBox) throws SQLException {
            return inBox.getByte();
        }

        @Override
        public void setValue(final OutBox param, final Byte value) throws SQLException {
            param.setByte(value);
        }
    };

    /**
     * Maps to {@link Float}.
     */
    public static final Mapper<Float> FLOAT = new Mapper<Float>() {
        @Override
        public Float value(final InBox inBox) throws SQLException {
            return inBox.getFloat();
        }

        @Override
        public void setValue(final OutBox param, final Float value) throws SQLException {
            param.setFloat(value);
        }
    };

    /**
     * Maps to array of bytes.
     * May be used for BLOBs.
     */
    public static final Mapper<byte[]> BYTES = new Mapper<byte[]>() {
        @Override
        public byte[] value(final InBox inBox) throws SQLException {
            return inBox.getBytes();
        }

        @Override
        public void setValue(final OutBox param, final byte[] value) throws SQLException {
            param.setBytes(value);
        }
    };
}
