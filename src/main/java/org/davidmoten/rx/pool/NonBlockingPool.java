package org.davidmoten.rx.pool;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;

public final class NonBlockingPool<T> implements Pool<T> {

    final Callable<T> factory;
    final Predicate<T> healthy;
    final long idleTimeBeforeHealthCheckMs;
    final Consumer<T> disposer;
    final int maxSize;
    final long maxIdleTimeMs;
    final long checkoutRetryIntervalMs;
    final long returnToPoolDelayAfterHealthCheckFailureMs;
    final long releaseIntervalMs;
    final BiFunction<T, Checkin, T> checkinDecorator;
    final Scheduler scheduler;

    private final AtomicReference<MemberSingle<T>> member = new AtomicReference<>();
    private final AtomicReference<List<Member<T>>> list = new AtomicReference<>(
            Collections.emptyList());
    private volatile boolean closed;

    private NonBlockingPool(Callable<T> factory, Predicate<T> healthy, Consumer<T> disposer,
            int maxSize, long returnToPoolDelayAfterHealthCheckFailureMs,
            long idleTimeBeforeHealthCheckMs, long maxIdleTimeMs, long checkoutRetryIntervalMs,
            long releaseIntervalMs, BiFunction<T, Checkin, T> checkinDecorator,
            Scheduler scheduler) {
        Preconditions.checkNotNull(factory);
        Preconditions.checkNotNull(healthy);
        Preconditions.checkNotNull(disposer);
        Preconditions.checkArgument(maxSize > 0);
        Preconditions.checkArgument(returnToPoolDelayAfterHealthCheckFailureMs >= 0);
        Preconditions.checkNotNull(checkinDecorator);
        Preconditions.checkNotNull(scheduler);
        Preconditions.checkArgument(checkoutRetryIntervalMs >= 0,
                "checkoutRetryIntervalMs must be >=0");
        Preconditions.checkArgument(releaseIntervalMs >= 0);
        this.factory = factory;
        this.healthy = healthy;
        this.disposer = disposer;
        this.maxSize = maxSize;
        this.returnToPoolDelayAfterHealthCheckFailureMs = returnToPoolDelayAfterHealthCheckFailureMs;
        this.idleTimeBeforeHealthCheckMs = idleTimeBeforeHealthCheckMs;
        this.maxIdleTimeMs = maxIdleTimeMs;
        this.checkoutRetryIntervalMs = checkoutRetryIntervalMs;
        this.releaseIntervalMs = releaseIntervalMs;
        this.checkinDecorator = checkinDecorator;
        this.scheduler = scheduler;// schedules retries
    }

    private MemberSingle<T> createMember() {
        return new MemberSingle<T>(this);
    }

    @Override
    public Single<Member<T>> member() {
        while (true) {
            MemberSingle<T> m = member.get();
            if (m != null)
                return m;
            else {
                m = createMember();
                if (member.compareAndSet(null, m)) {
                    return m;
                }
            }
        }
    }

    public void checkin(Member<T> m) {
        member.get().checkin(m);

    }

    @Override
    public void close() {
        closed = true;
        List<Member<T>> ms = list.getAndSet(null);
        if (ms != null) {
            for (Member<T> m : ms) {
                m.disposeValue();
            }
        }
    }

    boolean isClosed() {
        return closed;
    }

    public static <T> Builder<T> factory(Callable<T> factory) {
        return new Builder<T>().factory(factory);
    }

    public static class Builder<T> {

        private Callable<T> factory;
        private Predicate<T> healthy = x -> true;
        private long idleTimeBeforeHealthCheckMs = 30;
        private Consumer<T> disposer = new Consumer<T>() {
            @Override
            public void accept(T t) throws Exception {
                // do nothing
            }
        };
        private int maxSize = 10;
        private long returnToPoolDelayAfterHealthCheckFailureMs = 30000;
        private long checkoutRetryIntervalMs = 30000;
        private Scheduler scheduler = Schedulers.computation();
        private long maxIdleTimeMs;
        private long releaseIntervalMs = TimeUnit.MINUTES.toMillis(30);
        private BiFunction<T, Checkin, T> checkinDecorator;

        private Builder() {
        }

        public Builder<T> factory(Callable<T> factory) {
            Preconditions.checkNotNull(factory);
            this.factory = factory;
            return this;
        }

        public Builder<T> healthy(Predicate<T> healthy) {
            Preconditions.checkNotNull(healthy);
            this.healthy = healthy;
            return this;
        }

        public Builder<T> idleTimeBeforeHealthCheckMs(long value) {
            Preconditions.checkArgument(value >= 0);
            this.idleTimeBeforeHealthCheckMs = value;
            return this;
        }

        public Builder<T> idleTimeBeforeHealthCheck(long value, TimeUnit unit) {
            return idleTimeBeforeHealthCheckMs(unit.toMillis(value));
        }

        public Builder<T> maxIdleTimeMs(long value) {
            this.maxIdleTimeMs = value;
            return this;
        }

        public Builder<T> maxIdleTime(long value, TimeUnit unit) {
            return maxIdleTimeMs(unit.toMillis(value));
        }

        public Builder<T> checkoutRetryIntervalMs(long value) {
            checkoutRetryIntervalMs = value;
            return this;
        }

        public Builder<T> checkoutRetryInterval(long value, TimeUnit unit) {
            return checkoutRetryIntervalMs(unit.toMillis(value));
        }

        public Builder<T> disposer(Consumer<T> disposer) {
            Preconditions.checkNotNull(disposer);
            this.disposer = disposer;
            return this;
        }

        public Builder<T> maxSize(int maxSize) {
            Preconditions.checkArgument(maxSize > 0);
            this.maxSize = maxSize;
            return this;
        }

        public Builder<T> returnToPoolDelayAfterHealthCheckFailureMs(long retryDelayMs) {
            Preconditions.checkArgument(retryDelayMs >= 0);
            this.returnToPoolDelayAfterHealthCheckFailureMs = retryDelayMs;
            return this;
        }

        public Builder<T> returnToPoolDelayAfterHealthCheckFailure(long value, TimeUnit unit) {
            Preconditions.checkNotNull(unit);
            return returnToPoolDelayAfterHealthCheckFailureMs(unit.toMillis(value));
        }

        public Builder<T> releaseIntervalMs(long value, TimeUnit unit) {
            Preconditions.checkArgument(value >= 0);
            Preconditions.checkNotNull(unit);
            this.releaseIntervalMs = unit.toMillis(value);
            return this;
        }

        public Builder<T> scheduler(Scheduler scheduler) {
            Preconditions.checkNotNull(scheduler);
            this.scheduler = scheduler;
            return this;
        }

        public Builder<T> checkinDecorator(BiFunction<T, Checkin, T> f) {
            this.checkinDecorator = f;
            return this;
        }

        public NonBlockingPool<T> build() {
            return new NonBlockingPool<T>(factory, healthy, disposer, maxSize,
                    returnToPoolDelayAfterHealthCheckFailureMs, idleTimeBeforeHealthCheckMs,
                    maxIdleTimeMs, checkoutRetryIntervalMs, releaseIntervalMs, checkinDecorator,
                    scheduler);
        }
    }

}
