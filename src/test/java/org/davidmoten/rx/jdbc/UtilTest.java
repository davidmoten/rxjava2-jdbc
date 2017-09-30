package org.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;

import javax.sql.DataSource;

import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import org.junit.Test;

public class UtilTest {

    @Test
    public void testIsHashCode() throws NoSuchMethodException, SecurityException {
        Method method = Object.class.getMethod("hashCode");
        assertTrue(Util.isHashCode(method, new Object[] {}));
    }

    @Test
    public void testNotHashCodeIfHasArgs() throws NoSuchMethodException, SecurityException {
        Method method = Object.class.getMethod("hashCode");
        assertFalse(Util.isHashCode(method, new Object[] { 12 }));
    }

    @Test
    public void testNotHashCodeIfMethodNameWrong() throws NoSuchMethodException, SecurityException {
        Method method = Object.class.getMethod("equals", Object.class);
        assertFalse(Util.isHashCode(method, new Object[] {}));
    }

    @Test
    public void testDoubleQuote() {
        String sql = "select \"FRED\" from tbl where name=?";
        assertEquals(1, Util.countQuestionMarkParameters(sql));
    }

    @Test
    public void testAutomapDateToLong() {
        assertEquals(100L, (long) Util.autoMap(new java.sql.Date(100), Long.class));
    }

    @Test
    public void testAutomapDateToBigInteger() {
        assertEquals(100L,
                ((BigInteger) Util.autoMap(new java.sql.Date(100), BigInteger.class)).longValue());
    }

    @Test
    public void testAutomapDateToInstant() {
        assertEquals(100L,
                ((Instant) Util.autoMap(new java.sql.Date(100), Instant.class)).toEpochMilli());
    }

    @Test
    public void testAutomapDateToString() {
        assertEquals(100L,
                ((java.sql.Date) Util.autoMap(new java.sql.Date(100), String.class)).getTime());
    }

    @Test
    public void testConnectionProviderFromDataSource() throws SQLException {
        DataSource d = mock(DataSource.class);
        Connection c = mock(Connection.class);
        ConnectionProvider cp = Util.connectionProvider(d);
        when(d.getConnection()).thenReturn(c);
        assertTrue(c==cp.get());
        cp.close();
    }
    
    @Test(expected = SQLRuntimeException.class)
    public void testGetConnectionFromDataSourceWhenThrows() throws SQLException {
        DataSource d = mock(DataSource.class);
        when(d.getConnection()).thenThrow(SQLException.class);
        Util.getConnection(d);
    }
}
