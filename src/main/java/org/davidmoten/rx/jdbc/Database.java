package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;

import org.davidmoten.rx.pool.Pool;

import io.reactivex.Flowable;

public class Database implements AutoCloseable {

    private final Pool<Connection> pool;

    public Database(Pool<Connection> pool) {
        this.pool = pool;
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

    public static Database from(Pool<Connection> pool) {
        return new Database(pool);
    }

    public Flowable<Connection> connections() {
        return pool.members().cast(Connection.class);
    }

    @Override
    public void close() throws Exception {
        pool.close();
    }

    public SelectBuilder select(String sql) {
        return new SelectBuilder(sql, connections());
    }
    
    public TransactedBuilder<Object> transacted() {
        //TODO
        return new TransactedBuilder<Object>(() -> null);
    }
    
    public static <T> TransactedBuilder<T> tx(Tx<T> tx) {
        return new TransactedBuilder(() -> tx);
    }

}
