package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.sql.DataSource;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.Util;
import org.davidmoten.rx.jdbc.pool.internal.HealthCheckPredicate;
import org.davidmoten.rx.jdbc.pool.internal.PooledConnection;
import org.davidmoten.rx.jdbc.pool.internal.SerializedConnectionListener;
import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.NonBlockingPool;
import org.davidmoten.rx.pool.Pool;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.internal.schedulers.ExecutorScheduler;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;

public final class NonBlockingConnectionPool implements Pool<Connection> {

    private final AtomicReference<NonBlockingPool<Connection>> pool = new AtomicReference<>();

    NonBlockingConnectionPool(org.davidmoten.rx.pool.NonBlockingPool.Builder<Connection> builder) {
        pool.set(builder.build());
    }

    public static Builder<NonBlockingConnectionPool> builder() {
        return new Builder<NonBlockingConnectionPool>(x -> x);
    }

    public static final class Builder<T> {

        private ConnectionProvider cp;
        private Predicate<? super Connection> healthCheck = c -> true;
        private int maxPoolSize = 5;
        private long idleTimeBeforeHealthCheckMs = 60000;
        private long maxIdleTimeMs = 30 * 60000;
        private long connectionRetryIntervalMs = 30000;
        private Consumer<? super Connection> disposer = Util::closeSilently;
        private Scheduler scheduler = null;
        private Properties properties = new Properties();
        private final Function<NonBlockingConnectionPool, T> transform;
        private String url;
        private Consumer<? super Optional<Throwable>> c;

        public Builder(Function<NonBlockingConnectionPool, T> transform) {
            this.transform = transform;
        }

        /**
         * Sets the provider of {@link Connection} objects to be used by the pool.
         * 
         * @param cp
         *            connection provider
         * @return this
         */
        public Builder<T> connectionProvider(ConnectionProvider cp) {
            this.cp = cp;
            return this;
        }

        /**
         * Sets the provider of {@link Connection} objects to be used by the pool.
         * 
         * @param ds
         *            dataSource that providers Connections
         * @return this
         */
        public Builder<T> connectionProvider(DataSource ds) {
            return connectionProvider(Util.connectionProvider(ds));
        }

        /**
         * Sets the jdbc url of the {@link Connection} objects to be used by the pool.
         * 
         * @param url
         *            jdbc url
         * @return this
         */
        public Builder<T> url(String url) {
            this.url = url;
            return this;
        }

        /**
         * Sets the JDBC properties that will be passed to
         * {@link java.sql.DriverManager#getConnection}. The properties will only be
         * used if the {@code url} has been set in the builder.
         * 
         * @param properties
         *            the jdbc properties
         * @return this
         */
        public Builder<T> properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Adds the given property specified by key and value to the JDBC properties
         * that will be passed to {@link java.sql.DriverManager#getConnection}. The
         * properties will only be used if the {@code url} has been set in the builder.
         * 
         * @param key
         *            property key
         * @param value
         *            property value
         * @return this
         */
        public Builder<T> property(Object key, Object value) {
            this.properties.put(key, value);
            return this;
        }

        /**
         * Sets the max time a {@link Connection} can be idle (checked in to pool)
         * before it is released from the pool (the Connection is closed).
         * 
         * @param duration
         *            the period before which an idle Connection is released from the
         *            pool (closed).
         * @param unit
         *            time unit
         * @return this
         */
        public Builder<T> maxIdleTime(long duration, TimeUnit unit) {
            this.maxIdleTimeMs = unit.toMillis(duration);
            return this;
        }

        /**
         * Sets the minimum time that a connection must be idle (checked in) before on
         * the next checkout its validity is checked using the health check function. If
         * the health check fails then the Connection is closed (ignoring error) and
         * released from the pool. Another Connection is then scheduled for creation
         * (using the createRetryInterval delay).
         * 
         * @param duration
         *            minimum time a connection must be idle before its validity is
         *            checked on checkout from the pool
         * @param unit
         *            time unit
         * @return this
         */
        public Builder<T> idleTimeBeforeHealthCheck(long duration, TimeUnit unit) {
            this.idleTimeBeforeHealthCheckMs = unit.toMillis(duration);
            return this;
        }

        /**
         * Sets the retry interval in the case that creating/reestablishing a
         * {@link Connection} for use in the pool fails.
         * 
         * @param duration
         *            Connection creation retry interval
         * @param unit
         *            time unit
         * @return this
         */
        public Builder<T> connectionRetryInterval(long duration, TimeUnit unit) {
            this.connectionRetryIntervalMs = unit.toMillis(duration);
            return this;
        }

        /**
         * Sets the health check for a Connection in the pool that is run only if the
         * time since the last checkout of this Connection finished is more than
         * idleTimeBeforeHealthCheck and a checkout of this Connection has just been
         * requested.
         * 
         * @param healthCheck
         *            check to run on Connection. Returns true if and only if the
         *            Connection is valid/healthy.
         * @return this
         */
        public Builder<T> healthCheck(Predicate<? super Connection> healthCheck) {
            this.healthCheck = healthCheck;
            return this;
        }

        /**
         * Sets the health check for a Connection in the pool that is run only if the
         * time since the last checkout of this Connection finished is more than
         * idleTimeBeforeHealthCheck and a checkout of this Connection has just been
         * requested.
         * 
         * @param databaseType
         *            the check to run is chosen based on the database type
         * @return this
         */
        public Builder<T> healthCheck(DatabaseType databaseType) {
            return healthCheck(databaseType.healthCheck());
        }

        /**
         * Sets the health check for a Connection in the pool that is run only if the
         * time since the last checkout of this Connection finished is more than
         * idleTimeBeforeHealthCheck and a checkout of this Connection has just been
         * requested.
         * 
         * @param sql
         *            sql to run to check the validity of the connection. If the sql is
         *            run without error then the connection is assumed healthy.
         * @return this
         */
        public Builder<T> healthCheck(String sql) {
            return healthCheck(new HealthCheckPredicate(sql));
        }

        /**
         * Sets a listener for connection success and failure. Success and failure
         * events are reported serially to the listener. If the consumer throws it will
         * be reported to {@code RxJavaPlugins.onError}. This consumer should not block
         * otherwise it will block the connection pool itself.
         * 
         * @param c
         *            listener for connection events
         * @return this
         */
        public Builder<T> connectionListener(Consumer<? super Optional<Throwable>> c) {
            Preconditions.checkArgument(c != null, "listener can only be set once");
            this.c = c;
            return this;
        }

        /**
         * Sets the maximum connection pool size. Default is 5.
         * 
         * @param maxPoolSize
         *            maximum number of connections in the pool
         * @return this
         */
        public Builder<T> maxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }

        /**
         * Sets the scheduler used for emitting connections (must be scheduled to
         * another thread to break the chain of stack calls otherwise can get
         * StackOverflowError) and for scheduling timeouts and retries. Defaults to
         * {@code Schedulers.from(Executors.newFixedThreadPool(maxPoolSize))}. Do not
         * set the scheduler to {@code Schedulers.trampoline()} because queries will
         * block waiting for timeout workers. Also, do not use a single-threaded
         * {@link Scheduler} because you may encounter {@link StackOverflowError}.
         * 
         * @param scheduler
         *            scheduler to use for emitting connections and for scheduling
         *            timeouts and retries. Defaults to
         *            {@code Schedulers.from(Executors.newFixedThreadPool(maxPoolSize))}.
         *            Do not use {@code Schedulers.trampoline()}.
         * @throws IllegalArgumentException
         *             if trampoline specified
         * @return this
         */
        public Builder<T> scheduler(Scheduler scheduler) {
            Preconditions.checkArgument(scheduler != Schedulers.trampoline(),
                    "do not use trampoline scheduler because of risk of stack overflow");
            this.scheduler = scheduler;
            return this;
        }

        public T build() {
            if (scheduler == null) {
                ExecutorService executor = Executors.newFixedThreadPool(maxPoolSize);
                scheduler = new ExecutorScheduler(executor);
            }
            if (url != null) {
                cp = Util.connectionProvider(url, properties);
            }
            Consumer<Optional<Throwable>> listener;
            if (c == null) {
                listener = null;
            } else {
                listener = new SerializedConnectionListener(c);
            }
            NonBlockingConnectionPool p = new NonBlockingConnectionPool(NonBlockingPool //
                    .factory(() -> {
                        try {
                            Connection con = cp.get();
                            if (listener != null) {
                                try {
                                    listener.accept(Optional.empty());
                                } catch (Throwable e) {
                                    RxJavaPlugins.onError(e);
                                }
                            }
                            return con;
                        } catch (Throwable e) {
                            if (listener != null) {
                                try {
                                    listener.accept(Optional.of(e));
                                } catch (Throwable e2) {
                                    RxJavaPlugins.onError(e2);
                                }
                            }
                            throw e;
                        }
                    }) //
                    .checkinDecorator((con, checkin) -> new PooledConnection(con, checkin)) //
                    .idleTimeBeforeHealthCheck(idleTimeBeforeHealthCheckMs, TimeUnit.MILLISECONDS) //
                    .maxIdleTime(maxIdleTimeMs, TimeUnit.MILLISECONDS) //
                    .createRetryInterval(connectionRetryIntervalMs, TimeUnit.MILLISECONDS) //
                    .scheduler(scheduler) //
                    .disposer(disposer)//
                    .healthCheck(healthCheck) //
                    .scheduler(scheduler) //
                    .maxSize(maxPoolSize) //
            );
            return transform.apply(p);
        }

    }

    @Override
    public Single<Member<Connection>> member() {
        return pool.get().member();
    }

    @Override
    public void close() {
        pool.get().close();
    }

}
