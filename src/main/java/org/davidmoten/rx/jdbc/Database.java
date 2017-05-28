package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import org.davidmoten.rx.jdbc.pool.Pools;
import org.davidmoten.rx.pool.Pool;

import io.reactivex.Flowable;
import io.reactivex.functions.Action;

public final class Database implements AutoCloseable {

    private final Flowable<Connection> connections;

    private final Action onClose;

    private Database(Flowable<Connection> connections, Action onClose) {
        this.connections = connections;
        this.onClose = onClose;
    }

    public static Database from(Flowable<Connection> connections, Action onClose) {
        return new Database(connections, onClose);
    }

    public static Database from(String url, int maxPoolSize) {
        return Database.from( //
                Pools.nonBlocking() //
                        .url(url) //
                        .maxPoolSize(maxPoolSize) //
                        .build());
    }

    public static Database from(Pool<Connection> pool) {
        return new Database(pool.members().cast(Connection.class), () -> pool.close());
    }

    public static Database test(int maxPoolSize) {
        return Database.from( //
                Pools.nonBlocking() //
                        .connectionProvider(testConnectionProvider()) //
                        .maxPoolSize(maxPoolSize) //
                        .build());
    }

    static ConnectionProvider testConnectionProvider() {
        return connectionProvider(nextUrl());
    }

    public static Database test() {
        return test(3);
    }

    private static void createDatabase(Connection c) {
        try {
            Sql.statements(Database.class.getResourceAsStream("/database-test.sql")).stream()
                    .forEach(x -> {
                        try {
                            c.prepareStatement(x).execute();
                        } catch (SQLException e) {
                            throw new SQLRuntimeException(e);
                        }
                    });
            c.commit();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    private static ConnectionProvider connectionProvider(String url) {
        return new ConnectionProvider() {

            private final AtomicBoolean once = new AtomicBoolean(false);

            @Override
            public Connection get() {
                try {
                    Connection c = DriverManager.getConnection(url);
                    synchronized (this) {
                        if (once.compareAndSet(false, true)) {
                            createDatabase(c);
                        }
                    }
                    return c;
                } catch (SQLException e) {
                    throw new SQLRuntimeException(e);
                }
            }

            @Override
            public void close() {
                //
            }
        };
    }

    private static final AtomicInteger testDbNumber = new AtomicInteger();

    private static String nextUrl() {
        return "jdbc:derby:memory:derby" + testDbNumber.incrementAndGet() + ";create=true";
    }

    public Flowable<Connection> connections() {
        return connections;
    }

    @Override
    public void close() {
        try {
            onClose.run();
        } catch (Exception e) {
            throw new SQLRuntimeException(e);
        }
    }

    public SelectBuilder select(Class<?> cls) {
        // return new SelectBuilder(null, connections());
        // TODO
        throw new UnsupportedOperationException();
    }

    public SelectBuilder select(String sql) {
        return new SelectBuilder(sql, connections(), this);
    }
    
    public UpdateBuilder update(String sql) {
        return new UpdateBuilder(sql, connections(), this);
    }

    public TransactedBuilder tx(Tx<?> tx) {
        TxImpl<?> t = (TxImpl<?>) tx;
        TransactedConnection c = t.connection().fork();
        return new TransactedBuilder(c, this);
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

}
