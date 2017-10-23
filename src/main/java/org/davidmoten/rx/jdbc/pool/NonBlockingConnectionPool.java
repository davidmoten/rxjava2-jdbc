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

        public Builder<T> connectionProvider(ConnectionProvider cp) {
            this.cp = cp;
            return this;
        }

        public Builder<T> connectionProvider(DataSource d) {
            return connectionProvider(Util.connectionProvider(d));
        }

        public Builder<T> url(String url) {
            return connectionProvider(Util.connectionProvider(url));
        }

        public Builder<T> maxIdleTimeMs(long value) {
            this.maxIdleTimeMs = value;
            return this;
        }

        public Builder<T> maxIdleTime(long value, TimeUnit unit) {
            return maxIdleTimeMs(unit.toMillis(value));
        }

        public Builder<T> idleTimeBeforeHealthCheckMs(long value) {
            Preconditions.checkArgument(value >= 0);
            this.idleTimeBeforeHealthCheckMs = value;
            return this;
        }

        public Builder<T> createRetryIntervalMs(long value) {
            this.createRetryIntervalMs = value;
            return this;
        }

        public Builder<T> createRetryInterval(long value, TimeUnit unit) {
            return createRetryIntervalMs(unit.toMillis(value));
        }

        public Builder<T> idleTimeBeforeHealthCheck(long value, TimeUnit unit) {
            return idleTimeBeforeHealthCheckMs(unit.toMillis(value));
        }

        public Builder<T> healthCheck(Predicate<? super Connection> healthCheck) {
            this.healthCheck = healthCheck;
            return this;
        }

        public Builder<T> healthCheck(DatabaseType databaseType) {
            return healthCheck(databaseType.healthCheck());
        }

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
                    .idleTimeBeforeHealthCheckMs(idleTimeBeforeHealthCheckMs) //
                    .maxIdleTimeMs(maxIdleTimeMs) //
                    .createRetryIntervalMs(createRetryIntervalMs) //
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
