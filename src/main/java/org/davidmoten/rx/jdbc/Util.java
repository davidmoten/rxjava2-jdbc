package org.davidmoten.rx.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.functions.Consumer;

public enum Util {
    ;

    private static final Logger log = LoggerFactory.getLogger(Util.class);

    public static void closeAll(ResultSet rs) {
        Statement stmt = null;
        try {
            stmt = rs.getStatement();
        } catch (SQLException e) {
            // ignore
        }
        try {
            rs.close();
        } catch (SQLException e) {
            // ignore
        }
        if (stmt != null) {
            closeAll(stmt);
        }

    }

    public static void closeAll(Statement stmt) {
        try {
            stmt.close();
        } catch (SQLException e) {
            // ignore
        }
        Connection con = null;
        try {
            con = stmt.getConnection();
        } catch (SQLException e1) {
            // ignore
        }
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Sets parameters for the {@link PreparedStatement}.
     * 
     * @param ps
     * @param params
     * @throws SQLException
     */
    static void setParameters(PreparedStatement ps, List<Parameter> params, boolean namesAllowed)
            throws SQLException {
        for (int i = 1; i <= params.size(); i++) {
            if (params.get(i - 1).hasName() && !namesAllowed)
                throw new SQLException("named parameter found but sql does not contain names");
            Object o = params.get(i - 1).value();
            try {
                if (o == null)
                    ps.setObject(i, null);
                else if (o == Database.NULL_CLOB)
                    ps.setNull(i, Types.CLOB);
                else if (o == Database.NULL_BLOB)
                    ps.setNull(i, Types.BLOB);
                else {
                    Class<?> cls = o.getClass();
                    if (Clob.class.isAssignableFrom(cls)) {
                        setClob(ps, i, o, cls);
                    } else if (Blob.class.isAssignableFrom(cls)) {
                        setBlob(ps, i, o, cls);
                    } else if (Calendar.class.isAssignableFrom(cls)) {
                        Calendar cal = (Calendar) o;
                        Timestamp t = new java.sql.Timestamp(cal.getTimeInMillis());
                        ps.setTimestamp(i, t, cal);
                    } else if (Time.class.isAssignableFrom(cls)) {
                        Calendar cal = Calendar.getInstance();
                        ps.setTime(i, (Time) o, cal);
                    } else if (Timestamp.class.isAssignableFrom(cls)) {
                        Calendar cal = Calendar.getInstance();
                        ps.setTimestamp(i, (Timestamp) o, cal);
                    } else if (java.sql.Date.class.isAssignableFrom(cls)) {
                        Calendar cal = Calendar.getInstance();
                        ps.setDate(i, (java.sql.Date) o, cal);
                    } else if (java.util.Date.class.isAssignableFrom(cls)) {
                        Calendar cal = Calendar.getInstance();
                        java.util.Date date = (java.util.Date) o;
                        ps.setTimestamp(i, new java.sql.Timestamp(date.getTime()), cal);
                    } else
                        ps.setObject(i, o);
                }
            } catch (SQLException e) {
                log.debug("{} when setting ps.setObject({},{})", e.getMessage(), i, o);
                throw e;
            }
        }
    }

    /**
     * Sets a blob parameter for the prepared statement.
     * 
     * @param ps
     * @param i
     * @param o
     * @param cls
     * @throws SQLException
     */
    private static void setBlob(PreparedStatement ps, int i, Object o, Class<?> cls)
            throws SQLException {
        final InputStream is;
        if (o instanceof byte[]) {
            is = new ByteArrayInputStream((byte[]) o);
        } else if (o instanceof InputStream)
            is = (InputStream) o;
        else
            throw new RuntimeException(
                    "cannot insert parameter of type " + cls + " into blob column " + i);
        Blob c = ps.getConnection().createBlob();
        OutputStream os = c.setBinaryStream(1);
        copy(is, os);
        ps.setBlob(i, c);
    }

    /**
     * Sets the clob parameter for the prepared statement.
     * 
     * @param ps
     * @param i
     * @param o
     * @param cls
     * @throws SQLException
     */
    private static void setClob(PreparedStatement ps, int i, Object o, Class<?> cls)
            throws SQLException {
        final Reader r;
        if (o instanceof String)
            r = new StringReader((String) o);
        else if (o instanceof Reader)
            r = (Reader) o;
        else
            throw new RuntimeException(
                    "cannot insert parameter of type " + cls + " into clob column " + i);
        Clob c = ps.getConnection().createClob();
        Writer w = c.setCharacterStream(1);
        copy(r, w);
        ps.setClob(i, c);
    }

    /**
     * Copies a {@link Reader} to a {@link Writer}.
     * 
     * @param input
     * @param output
     * @return
     */
    private static int copy(Reader input, Writer output) {
        try {
            return IOUtils.copy(input, output);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Copies an {@link InputStream} to an {@link OutputStream}.
     * 
     * @param input
     * @param output
     * @return
     */
    private static int copy(InputStream input, OutputStream output) {
        try {
            return IOUtils.copy(input, output);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setNamedParameters(PreparedStatement ps, List<Parameter> parameters,
            List<String> names) throws SQLException {
        Map<String, Parameter> map = new HashMap<String, Parameter>();
        for (Parameter p : parameters) {
            if (p.hasName()) {
                map.put(p.name(), p);
            } else {
                throw new ParameterMissingNameException(
                        "named parameters were expected but this parameter did not have a name: "
                                + p);
            }
        }
        List<Parameter> list = new ArrayList<Parameter>();
        for (String name : names) {
            if (!map.containsKey(name))
                throw new SQLException("named parameter is missing for '" + name + "'");
            Parameter p = map.get(name);
            list.add(p);
        }
        Util.setParameters(ps, list, true);
    }

    static PreparedStatement setParameters(PreparedStatement ps, List<Object> parameters,
            List<String> names) throws SQLException {
        List<Parameter> params = parameters.stream().map(o -> {
            if (o instanceof Parameter) {
                return (Parameter) o;
            } else {
                return new Parameter(o);
            }
        }).collect(Collectors.toList());
        if (names.isEmpty()) {
            Util.setParameters(ps, params, false);
        } else {
            Util.setNamedParameters(ps, params, names);
        }
        return ps;
    }

    public static void closeSilently(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception e) {
            // ignore
        }
    }

    static void closePreparedStatementAndConnection(PreparedStatement ps) {
        Connection con = null;
        try {
            con = ps.getConnection();
        } catch (SQLException e) {
        }
        closeSilently(ps);
        if (con != null) {
            closeSilently(con);
        }
    }

    static void closePreparedStatementAndConnection(NamedPreparedStatement ps) {
        closePreparedStatementAndConnection(ps.ps);
    }

    static NamedPreparedStatement prepare(Connection con, String sql) throws SQLException {
        SqlInfo s = SqlInfo.parse(sql);
        return new NamedPreparedStatement(con.prepareStatement(s.sql(), ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY), s.names());
    }

    static NamedPreparedStatement prepareReturnGeneratedKeys(Connection con, String sql)
            throws SQLException {
        SqlInfo s = SqlInfo.parse(sql);
        return new NamedPreparedStatement(
                con.prepareStatement(s.sql(), Statement.RETURN_GENERATED_KEYS), s.names());
    }

    // Visible for testing
    static int countQuestionMarkParameters(String sql) {
        // was originally using regular expressions, but they didn't work well
        // for ignoring parameter-like strings inside quotes.
        int count = 0;
        int length = sql.length();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = 0; i < length; i++) {
            char c = sql.charAt(i);
            if (inSingleQuote) {
                if (c == '\'') {
                    inSingleQuote = false;
                }
            } else if (inDoubleQuote) {
                if (c == '"') {
                    inDoubleQuote = false;
                }
            } else {
                if (c == '\'') {
                    inSingleQuote = true;
                } else if (c == '"') {
                    inDoubleQuote = true;
                } else if (c == '?') {
                    count++;
                }
            }
        }
        return count;
    }

    public static void commit(PreparedStatement ps) throws SQLException {
        ps.getConnection().commit();
    }

    public static void rollback(PreparedStatement ps) throws SQLException {
        ps.getConnection().rollback();
    }

    public static final Consumer<PreparedStatement> CLOSE_PS_AND_CONNECTION = new Consumer<PreparedStatement>() {

        @Override
        public void accept(PreparedStatement ps) throws Exception {
            Util.closePreparedStatementAndConnection(ps);
        }
    };

    // auto-mapping

    /**
     * Converts from java.sql Types to common java types like java.util.Date and
     * numeric types.
     * 
     * @param o
     * @param cls
     * @return
     */
    public static Object autoMap(Object o, Class<?> cls) {
        if (o == null)
            return o;
        else if (cls.isAssignableFrom(o.getClass())) {
            return o;
        } else {
            if (o instanceof java.sql.Date) {
                java.sql.Date d = (java.sql.Date) o;
                if (cls.isAssignableFrom(Long.class))
                    return d.getTime();
                else if (cls.isAssignableFrom(BigInteger.class))
                    return BigInteger.valueOf(d.getTime());
                else
                    return o;
            } else if (o instanceof java.sql.Timestamp) {
                Timestamp t = (java.sql.Timestamp) o;
                if (cls.isAssignableFrom(Long.class))
                    return t.getTime();
                else if (cls.isAssignableFrom(BigInteger.class))
                    return BigInteger.valueOf(t.getTime());
                else
                    return o;
            } else if (o instanceof java.sql.Time) {
                Time t = (java.sql.Time) o;
                if (cls.isAssignableFrom(Long.class))
                    return t.getTime();
                else if (cls.isAssignableFrom(BigInteger.class))
                    return BigInteger.valueOf(t.getTime());
                else
                    return o;
            } else if (o instanceof Blob && cls.isAssignableFrom(byte[].class)) {
                return toBytes((Blob) o);
            } else if (o instanceof Clob && cls.isAssignableFrom(String.class)) {
                return toString((Clob) o);
            } else if (o instanceof BigInteger && cls.isAssignableFrom(Long.class)) {
                return ((BigInteger) o).longValue();
            } else if (o instanceof BigInteger && cls.isAssignableFrom(Integer.class)) {
                return ((BigInteger) o).intValue();
            } else if (o instanceof BigInteger && cls.isAssignableFrom(Double.class)) {
                return ((BigInteger) o).doubleValue();
            } else if (o instanceof BigInteger && cls.isAssignableFrom(Float.class)) {
                return ((BigInteger) o).floatValue();
            } else if (o instanceof BigInteger && cls.isAssignableFrom(Short.class)) {
                return ((BigInteger) o).shortValue();
            } else if (o instanceof BigInteger && cls.isAssignableFrom(BigDecimal.class)) {
                return new BigDecimal((BigInteger) o);
            } else if (o instanceof BigDecimal && cls.isAssignableFrom(Double.class)) {
                return ((BigDecimal) o).doubleValue();
            } else if (o instanceof BigDecimal && cls.isAssignableFrom(Integer.class)) {
                return ((BigDecimal) o).toBigInteger().intValue();
            } else if (o instanceof BigDecimal && cls.isAssignableFrom(Float.class)) {
                return ((BigDecimal) o).floatValue();
            } else if (o instanceof BigDecimal && cls.isAssignableFrom(Short.class)) {
                return ((BigDecimal) o).toBigInteger().shortValue();
            } else if (o instanceof BigDecimal && cls.isAssignableFrom(Long.class)) {
                return ((BigDecimal) o).toBigInteger().longValue();
            } else if (o instanceof BigDecimal && cls.isAssignableFrom(BigInteger.class)) {
                return ((BigDecimal) o).toBigInteger();
            } else if ((o instanceof Short || o instanceof Integer || o instanceof Long)
                    && cls.isAssignableFrom(BigInteger.class)) {
                return new BigInteger(o.toString());
            } else if (o instanceof Number && cls.isAssignableFrom(BigDecimal.class)) {
                return new BigDecimal(o.toString());
            } else if (o instanceof Number && cls.isAssignableFrom(Short.class))
                return ((Number) o).shortValue();
            else if (o instanceof Number && cls.isAssignableFrom(Integer.class))
                return ((Number) o).intValue();
            else if (o instanceof Number && cls.isAssignableFrom(Integer.class))
                return ((Number) o).intValue();
            else if (o instanceof Number && cls.isAssignableFrom(Long.class))
                return ((Number) o).longValue();
            else if (o instanceof Number && cls.isAssignableFrom(Float.class))
                return ((Number) o).floatValue();
            else if (o instanceof Number && cls.isAssignableFrom(Double.class))
                return ((Number) o).doubleValue();
            else
                return o;
        }
    }

    public static <T> T mapObject(final ResultSet rs, Class<T> cls, int i) {
        return (T) autoMap(getObject(rs, cls, i), cls);
    }

    private static <T> Object getObject(final ResultSet rs, Class<T> cls, int i) {
        try {
            if (rs.getObject(i) == null) {
                return null;
            }
            final int type = rs.getMetaData().getColumnType(i);
            // TODO java.util.Calendar support
            // TODO XMLGregorian Calendar support
            if (type == Types.DATE)
                return rs.getDate(i, Calendar.getInstance());
            else if (type == Types.TIME)
                return rs.getTime(i, Calendar.getInstance());
            else if (type == Types.TIMESTAMP)
                return rs.getTimestamp(i, Calendar.getInstance());
            else if (type == Types.CLOB && cls.equals(String.class)) {
                return toString(rs.getClob(i));
            } else if (type == Types.CLOB && Reader.class.isAssignableFrom(cls)) {
                Clob c = rs.getClob(i);
                Reader r = c.getCharacterStream();
                return createFreeOnCloseReader(c, r);
            } else if (type == Types.BLOB && cls.equals(byte[].class)) {
                return toBytes(rs.getBlob(i));
            } else if (type == Types.BLOB && InputStream.class.isAssignableFrom(cls)) {
                final Blob b = rs.getBlob(i);
                final InputStream is = rs.getBlob(i).getBinaryStream();
                return createFreeOnCloseInputStream(b, is);
            } else
                return rs.getObject(i);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    /**
     * Returns the bytes of a {@link Blob} and frees the blob resource.
     * 
     * @param b
     *            blob
     * @return
     */
    private static byte[] toBytes(Blob b) {
        try {
            InputStream is = b.getBinaryStream();
            byte[] result = IOUtils.toByteArray(is);
            is.close();
            b.free();
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }

    }

    /**
     * Returns the String of a {@link Clob} and frees the clob resource.
     * 
     * @param c
     * @return
     */
    private static String toString(Clob c) {
        try {
            Reader reader = c.getCharacterStream();
            String result = IOUtils.toString(reader);
            reader.close();
            c.free();
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    /**
     * Automatically frees the blob (<code>blob.free()</code>) once the blob
     * {@link InputStream} is closed.
     * 
     * @param blob
     * @param is
     * @return
     */
    private static InputStream createFreeOnCloseInputStream(final Blob blob, final InputStream is) {
        return new InputStream() {

            @Override
            public int read() throws IOException {
                return is.read();
            }

            @Override
            public void close() throws IOException {
                try {
                    is.close();
                } finally {
                    try {
                        blob.free();
                    } catch (SQLException e) {
                        log.debug(e.getMessage());
                    }
                }
            }
        };
    }

    /**
     * Automatically frees the clob (<code>Clob.free()</code>) once the clob
     * Reader is closed.
     * 
     * @param clob
     * @param reader
     * @return
     */
    private static Reader createFreeOnCloseReader(final Clob clob, final Reader reader) {
        return new Reader() {

            @Override
            public void close() throws IOException {
                try {
                    reader.close();
                } finally {
                    try {
                        clob.free();
                    } catch (SQLException e) {
                        log.debug(e.getMessage());
                    }
                }
            }

            @Override
            public int read(char[] cbuf, int off, int len) throws IOException {
                return reader.read(cbuf, off, len);
            }
        };
    }
}
