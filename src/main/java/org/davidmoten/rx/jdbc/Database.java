package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import org.davidmoten.rx.jdbc.pool.Pools;
import org.davidmoten.rx.pool.Pool;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.functions.Action;

public final class Database implements AutoCloseable {

    private final Flowable<Connection> connections;

    private final Action onClose;

    private Database(@Nonnull Flowable<Connection> connections, @Nonnull Action onClose) {
        this.connections = connections;
        this.onClose = onClose;
    }

    public static Database from(@Nonnull Flowable<Connection> connections, @Nonnull Action onClose) {
        Preconditions.checkNotNull(connections, "connections cannot be null");
        Preconditions.checkNotNull(onClose, "onClose cannot be null");
        return new Database(connections, onClose);
    }

    public static Database from(@Nonnull String url, int maxPoolSize) {
        Preconditions.checkNotNull(url, "url cannot be null");
        Preconditions.checkArgument(maxPoolSize > 0, "maxPoolSize must be greater than 0");
        return Database.from( //
                Pools.nonBlocking() //
                        .url(url) //
                        .maxPoolSize(maxPoolSize) //
                        .build());
    }

    public static Database from(@Nonnull Pool<Connection> pool) {
        Preconditions.checkNotNull(pool, "pool canot be null");
        return new Database(pool.members().cast(Connection.class), () -> pool.close());
    }

    public static Database test(int maxPoolSize) {
        Preconditions.checkArgument(maxPoolSize > 0, "maxPoolSize must be greater than 0");
        return Database.from( //
                Pools.nonBlocking() //
                        .connectionProvider(testConnectionProvider()) //
                        .maxPoolSize(maxPoolSize) //
                        .build());
    }

    static ConnectionProvider testConnectionProvider() {
        return connectionProvider(nextUrl());
    }

    /**
     * Returns a new testing apache derby in-memory database with a connection
     * pool of size 3.
     * 
     * @return new testing Database instance
     */
    public static Database test() {
        return test(3);
    }

    private static void createDatabase(@Nonnull Connection c) {
        try {
            Sql //
                    .statements(Database.class.getResourceAsStream("/database-test.sql")) //
                    .stream() //
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

    private static ConnectionProvider connectionProvider(@Nonnull String url) {
        return new ConnectionProvider() {

            private final AtomicBoolean once = new AtomicBoolean();
            private final CountDownLatch latch = new CountDownLatch(1);

            @Override
            public Connection get() {
                try {
                    Connection c = DriverManager.getConnection(url);
                    if (once.compareAndSet(false, true)) {
                        createDatabase(c);
                        latch.countDown();
                    } else {
                        latch.await(1, TimeUnit.MINUTES);
                    }
                    return c;
                } catch (SQLException e) {
                    throw new SQLRuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
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

    public <T> SelectAutomappedBuilder<T> select(@Nonnull Class<T> cls) {
        Preconditions.checkNotNull(cls, "cls cannot be null");
        return new SelectAutomappedBuilder<T>(cls, connections, this);
    }

    public SelectBuilder select(@Nonnull String sql) {
        Preconditions.checkNotNull(sql, "sql cannot be null");
        return new SelectBuilder(sql, connections(), this);
    }

    public UpdateBuilder update(@Nonnull String sql) {
        Preconditions.checkNotNull(sql, "sql cannot be null");
        return new UpdateBuilder(sql, connections(), this);
    }

    public TransactedBuilder tx(@Nonnull Tx<?> tx) {
        Preconditions.checkNotNull(tx, "tx cannot be null");
        TxImpl<?> t = (TxImpl<?>) tx;
        TransactedConnection c = t.connection().fork();
        return new TransactedBuilder(c, this);
    }

    public static final Object NULL_CLOB = new Object();

    public static final Object NULL_NUMBER = new Object();

    public static Object toSentinelIfNull(@Nullable String s) {
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

    public static Object toSentinelIfNull(@Nullable byte[] bytes) {
        if (bytes == null)
            return NULL_BLOB;
        else
            return bytes;
    }

    public static Object clob(@Nullable String s) {
        return toSentinelIfNull(s);
    }

    public static Object blob(@Nullable byte[] bytes) {
        return toSentinelIfNull(bytes);
    }

}
