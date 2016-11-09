package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;

public class Database implements AutoCloseable {

    private final ConnectionProvider cp;

    public Database(ConnectionProvider cp) {
        this.cp = cp;
    }

    public static final Object NULL_CLOB = new Object();

    public static final Object NULL_NUMBER = new Object();

    public static Object toSentinelIfNull(String s) {
        if (s == null)
            return NULL_CLOB;
        else
            return s;
    }

    /**
     * Sentinel object used to indicate in parameters of a query that rather
     * than calling {@link PreparedStatement#setObject(int, Object)} with a null
     * we call {@link PreparedStatement#setNull(int, int)} with
     * {@link Types#CLOB}. This is required by many databases for setting CLOB
     * and BLOB fields to null.
     */
    public static final Object NULL_BLOB = new Object();

    public static Object toSentinelIfNull(byte[] bytes) {
        if (bytes == null)
            return NULL_BLOB;
        else
            return bytes;
    }

    public static Database from(ConnectionProvider cp) {
        return new Database(cp);
    }

    public static Database from(Connection con) {
        return new Database(new ConnectionProvider() {

            @Override
            public Connection get() {
                return con;
            }

            @Override
            public void close() {
                Util.closeSilently(con);
            }
        });
    }

    @Override
    public void close() throws Exception {
        cp.close();
    }

}
