package org.symqle.jdbctest;

import junit.framework.TestCase;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.StatementParameters;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;


/**
 * @author lvovich
 */
public class StatementParametersTest extends TestCase {

    public void testLong() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        statement.setLong(1, 12L);
        statement.setNull(2, Types.BIGINT);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setLong(12L);
        params.next().setLong(null);

        verify(statement);
    }

    public void testInt() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        statement.setInt(1, 12);
        statement.setNull(2, Types.INTEGER);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setInt(12);
        params.next().setInt(null);

        verify(statement);
    }

    public void testShort() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        statement.setShort(1, (short) 12);
        statement.setNull(2, Types.SMALLINT);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setShort((short) 12);
        params.next().setShort(null);

        verify(statement);

    }

    public void testByte() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        statement.setByte(1, (byte) 12);
        statement.setNull(2, Types.TINYINT);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setByte((byte) 12);
        params.next().setByte(null);

        verify(statement);

    }

    public void testBigDecimal()  throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final BigDecimal big = new BigDecimal(123.456);
        statement.setBigDecimal(1, big);
        statement.setBigDecimal(2, null);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setBigDecimal(big);
        params.next().setBigDecimal(null);

        verify(statement);

    }

    public void testBoolean() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        statement.setBoolean(1, true);
        statement.setNull(2, Types.BOOLEAN);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setBoolean(true);
        params.next().setBoolean(null);

        verify(statement);

    }

    public void testString() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        statement.setString(1, "test");
        statement.setString(2, null);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setString("test");
        params.next().setString(null);

        verify(statement);


    }

    public void testDate() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final Date now = new Date(System.currentTimeMillis());
        statement.setDate(1, now);
        statement.setDate(2, null);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setDate(now);
        params.next().setDate(null);

        verify(statement);

    }

    public void testTimestamp() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        statement.setTimestamp(1, now);
        statement.setTimestamp(2, null);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setTimestamp(now);
        params.next().setTimestamp(null);

        verify(statement);

    }

    public void testTime() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final Time time = new Time(3600000);
        statement.setTime(1, time);
        statement.setTime(2, null);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setTime(time);
        params.next().setTime(null);

        verify(statement);
    }

    public void testDouble() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final Double test = 123.456;
        statement.setDouble(1, test);
        statement.setNull(2, Types.DOUBLE);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setDouble(test);
        params.next().setDouble(null);

        verify(statement);

    }

    public void testFloat() throws Exception {
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final Float test = (float) 123.456;
        statement.setFloat(1, test);
        statement.setNull(2, Types.FLOAT);

        replay(statement);

        SqlParameters params = new StatementParameters(statement);
        params.next().setFloat(test);
        params.next().setFloat(null);

        verify(statement);

    }

}
