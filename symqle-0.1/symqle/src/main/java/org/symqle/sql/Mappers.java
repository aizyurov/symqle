package org.symqle.sql;

import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
import org.symqle.common.Mapper;
import org.symqle.common.OutBox;

import java.sql.SQLException;

/**
 * @author lvovich
 */
public class Mappers implements CoreMappers {

    static {
        new Mappers();
    }

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
