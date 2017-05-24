package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.Util;
import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.NonBlockingPool;
import org.davidmoten.rx.pool.Pool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;

public final class NonBlockingConnectionPool implements Pool<Connection> {

    private static final Logger log = LoggerFactory.getLogger(NonBlockingConnectionPool.class);

    private final AtomicReference<NonBlockingPool<Connection>> pool = new AtomicReference<NonBlockingPool<Connection>>();

    private volatile boolean closed;

    public NonBlockingConnectionPool(org.davidmoten.rx.pool.NonBlockingPool.Builder<Connection> builder) {
        pool.set(builder.memberFactory(p -> new ConnectionNonBlockingMember(pool.get())).build());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private ConnectionProvider cp;
        private Predicate<Connection> healthy = c -> true;
        private int maxPoolSize = 5;
        private long returnToPoolDelayAfterHealthCheckFailureMs = 1000;
        private long idleTimeBeforeHealthCheckMs = 60000;
        private long maxIdleTimeMs = 30 * 60000;
        private Consumer<Connection> disposer = Util::closeSilently;
        private Scheduler scheduler = Schedulers.io();

        public Builder connectionProvider(ConnectionProvider cp) {
            this.cp = cp;
            return this;
        }

        public Builder url(String url) {
            return connectionProvider(Util.connectionProvider(url));
        }

        public Builder maxIdleTimeMs(long value) {
            this.maxIdleTimeMs = value;
            return this;
        }

        public Builder maxIdleTime(long value, TimeUnit unit) {
            return maxIdleTimeMs(unit.toMillis(value));
        }

        public Builder idleTimeBeforeHealthCheckMs(long value) {
            Preconditions.checkArgument(value >= 0);
            this.idleTimeBeforeHealthCheckMs = value;
            return this;
        }

        public Builder idleTimeBeforeHealthCheck(long value, TimeUnit unit) {
            return idleTimeBeforeHealthCheckMs(unit.toMillis(value));
        }

        public Builder healthy(Predicate<Connection> healthy) {
            this.healthy = healthy;
            return this;
        }

        public Builder maxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }

        public Builder returnToPoolDelayAfterHealthCheckFailureMs(long value) {
            this.returnToPoolDelayAfterHealthCheckFailureMs = value;
            return this;
        }

        public Builder returnToPoolDelayAfterHealthCheckFailure(long value, TimeUnit unit) {
            return returnToPoolDelayAfterHealthCheckFailureMs(unit.toMillis(value));
        }

        public Builder scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public NonBlockingConnectionPool build() {
            return new NonBlockingConnectionPool(NonBlockingPool //
                    .factory(() -> cp.get()) //
                    .idleTimeBeforeHealthCheckMs(idleTimeBeforeHealthCheckMs) //
                    .maxIdleTimeMs(maxIdleTimeMs) //
                    .scheduler(Schedulers.io()) //
                    .disposer(disposer)//
                    .healthy(healthy) //
                    .scheduler(scheduler) //
                    .maxSize(maxPoolSize) //
                    .returnToPoolDelayAfterHealthCheckFailureMs(returnToPoolDelayAfterHealthCheckFailureMs)); //
        }

    }

    @Override
    public Flowable<Member<Connection>> members() {
        return pool.get().members() //
                .doOnNext(m -> {
                    if (closed) {
                        throw new PoolClosedException();
                    }
                }) //
                .doOnRequest(n -> log.debug("connections requested={}", n)) //
                .doOnNext(c -> log.debug("supplied {}", c));
    }

    @Override
    public void close() {
        closed = true;
        pool.get().close();
    }

}
