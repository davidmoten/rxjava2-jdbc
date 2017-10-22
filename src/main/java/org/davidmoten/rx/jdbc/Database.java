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
import javax.sql.DataSource;

import org.davidmoten.rx.FlowableSingleDeferUntilRequest;
import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import org.davidmoten.rx.jdbc.pool.NonBlockingConnectionPool;
import org.davidmoten.rx.jdbc.pool.Pools;
import org.davidmoten.rx.jdbc.pool.internal.ConnectionProviderBlockingPool;
import org.davidmoten.rx.pool.Pool;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Action;

public final class Database implements AutoCloseable {

    private final Single<Connection> connection;

    private final Action onClose;

    private Database(@Nonnull Single<Connection> connection, @Nonnull Action onClose) {
        this.connection = connection;
        this.onClose = onClose;
    }

    /**
     * Creates a Database instance. This is a low-level creation method that needs a
     * lot of care so it is recommended to use the other static factory methods on
     * Database that will handle pooling for you.
     * 
     * @param connection
     *            each subscription to this Single should yield a valid open
     *            Connection to the database. Bear in mind that once a query is
     *            completed with this connection the connection will be closed. If
     *            you don't want the connection to be closed (pooling behaviour)
     *            then you need to override the `close()` behaviour of the
     *            Connection and handle the semaphore pool pattern yourself (!) or
     *            preferrably use the other static factory methods like
     *            {@code Database.from(url, maxPoolSize)} for instance.
     * @param onClose
     *            the action that will be run when {@code Database.close()} is
     *            called.
     * @return Database instance using the given connection provider
     */
    public static Database from(@Nonnull Single<Connection> connection, @Nonnull Action onClose) {
        Preconditions.checkNotNull(connection, "connections cannot be null");
        Preconditions.checkNotNull(onClose, "onClose cannot be null");
        return new Database(connection, onClose);
    }

    public static Database from(@Nonnull String url, int maxPoolSize) {
        Preconditions.checkNotNull(url, "url cannot be null");
        Preconditions.checkArgument(maxPoolSize > 0, "maxPoolSize must be greater than 0");
        NonBlockingConnectionPool pool = Pools.nonBlocking() //
                .url(url) //
                .maxPoolSize(maxPoolSize) //
                .build();
        return Database.from( //
                pool, //
                () -> {
                    pool.close();
                });
    }

    public static Database from(@Nonnull Pool<Connection> pool) {
        Preconditions.checkNotNull(pool, "pool canot be null");
        return new Database(pool.member().map(x -> {
            if (x.value() == null) {
                throw new NullPointerException("connection is null!");
            }
            return x.value();
        }), () -> pool.close());
    }

    public static Database from(@Nonnull Pool<Connection> pool, Action closeAction) {
        Preconditions.checkNotNull(pool, "pool canot be null");
        return new Database(pool.member().map(x -> {
            if (x.value() == null) {
                throw new NullPointerException("connection is null!");
            }
            return x.value();
        }), closeAction);
    }

    public static Database fromBlocking(@Nonnull ConnectionProvider cp) {
        return Database.from(new ConnectionProviderBlockingPool(cp));
    }

    public static Database fromBlocking(@Nonnull DataSource dataSource) {
        return fromBlocking(Util.connectionProvider(dataSource));
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
        return testConnectionProvider(nextUrl());
    }

    /**
     * Returns a new testing apache derby in-memory database with a connection pool
     * of size 3.
     * 
     * @return new testing Database instance
     */
    public static Database test() {
        return test(3);
    }

    private static void createTestDatabase(@Nonnull Connection c) {
        try {
            Sql //
                    .statements(Database.class.getResourceAsStream("/database-test.sql")) //
                    .stream() //
                    .forEach(x -> {
                        try (PreparedStatement s = c.prepareStatement(x)) {
                            s.execute();
                        } catch (SQLException e) {
                            throw new SQLRuntimeException(e);
                        }
                    });
            c.commit();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    private static ConnectionProvider testConnectionProvider(@Nonnull String url) {
        return new ConnectionProvider() {

            private final AtomicBoolean once = new AtomicBoolean();
            private final CountDownLatch latch = new CountDownLatch(1);

            @Override
            public Connection get() {
                try {
                    Connection c = DriverManager.getConnection(url);
                    if (once.compareAndSet(false, true)) {
                        createTestDatabase(c);
                        latch.countDown();
                    } else {
                        if (!latch.await(1, TimeUnit.MINUTES)) {
                            throw new SQLRuntimeException(
                                    "waited 1 minute but test database was not created");
                        }
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

    public Single<Connection> connection() {
        return connection;
    }

    /**
     * <p>
     * Returns a flowable stream of checked out Connections from the pool. It's
     * preferrable to use the {@code connection()} method and subscribe to a
     * MemberSingle instead because the sometimes surprising request patterns of
     * Flowable operators may mean that more Connections are checked out from the
     * pool than are needed. For instance if you use
     * 
     * <pre>
     * Flowable&lt;Connection&gt; cons = Database.connection().repeat()
     * </pre>
     * <p>
     * then you will checkout more (1 more) Connection with {@code repeat} than you
     * requested because {@code repeat} subscribes one more time than dictated by
     * the requests (buffers).
     * 
     * @return stream of checked out connections from the pool. When you call
     *         {@code close()} on a connection it is returned to the pool
     */
    public Flowable<Connection> connections() {
        return new FlowableSingleDeferUntilRequest<Connection>(connection).repeat();
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
        return new SelectAutomappedBuilder<T>(cls, connection, this);
    }

    public SelectBuilder select(@Nonnull String sql) {
        Preconditions.checkNotNull(sql, "sql cannot be null");
        return new SelectBuilder(sql, connection(), this);
    }

    public UpdateBuilder update(@Nonnull String sql) {
        Preconditions.checkNotNull(sql, "sql cannot be null");
        return new UpdateBuilder(sql, connection(), this);
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
     * Sentinel object used to indicate in parameters of a query that rather than
     * calling {@link PreparedStatement#setObject(int, Object)} with a null we call
     * {@link PreparedStatement#setNull(int, int)} with {@link Types#CLOB}. This is
     * required by many databases for setting CLOB and BLOB fields to null.
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
