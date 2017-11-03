package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
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
import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.NonBlockingPool;
import org.davidmoten.rx.pool.Pool;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.internal.schedulers.ExecutorScheduler;
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
        private long createRetryIntervalMs = 30000;
        private Consumer<? super Connection> disposer = Util::closeSilently;
        private Scheduler scheduler = null;
        private final Function<NonBlockingConnectionPool, T> transform;

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
            return connectionProvider(Util.connectionProvider(url));
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
        public Builder<T> createRetryInterval(long duration, TimeUnit unit) {
            this.createRetryIntervalMs = unit.toMillis(duration);
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
            NonBlockingConnectionPool p = new NonBlockingConnectionPool(NonBlockingPool //
                    .factory(() -> cp.get()) //
                    .checkinDecorator((con, checkin) -> new PooledConnection(con, checkin)) //
                    .idleTimeBeforeHealthCheck(idleTimeBeforeHealthCheckMs, TimeUnit.MILLISECONDS) //
                    .maxIdleTime(maxIdleTimeMs, TimeUnit.MILLISECONDS) //
                    .createRetryInterval(createRetryIntervalMs, TimeUnit.MILLISECONDS) //
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
