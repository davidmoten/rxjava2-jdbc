package org.davidmoten.rx.pool;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;

public final class NonBlockingPool2<T> implements Pool2<T> {

    private static final Logger log = LoggerFactory.getLogger(NonBlockingPool2.class);

    final Callable<T> factory;
    final Predicate<T> healthy;
    final long idleTimeBeforeHealthCheckMs;
    final Consumer<T> disposer;
    final int maxSize;
    final long maxIdleTimeMs;
    final long returnToPoolDelayAfterHealthCheckFailureMs;
    final MemberFactory2<T, NonBlockingPool2<T>> memberFactory;
    final Scheduler scheduler;

    private final AtomicReference<Single<Member2<T>>> member = new AtomicReference<>();
    private final AtomicReference<List<Member2<T>>> list = new AtomicReference<>(Collections.emptyList());

    private NonBlockingPool2(Callable<T> factory, Predicate<T> healthy, Consumer<T> disposer, int maxSize,
            long returnToPoolDelayAfterHealthCheckFailureMs, long idleTimeBeforeHealthCheckMs, long maxIdleTimeMs,
            MemberFactory2<T, NonBlockingPool2<T>> memberFactory, Scheduler scheduler) {
        Preconditions.checkNotNull(factory);
        Preconditions.checkNotNull(healthy);
        Preconditions.checkNotNull(disposer);
        Preconditions.checkArgument(maxSize > 0);
        Preconditions.checkArgument(returnToPoolDelayAfterHealthCheckFailureMs >= 0);
        Preconditions.checkNotNull(memberFactory);
        Preconditions.checkNotNull(scheduler);
        this.factory = factory;
        this.healthy = healthy;
        this.disposer = disposer;
        this.maxSize = maxSize;
        this.returnToPoolDelayAfterHealthCheckFailureMs = returnToPoolDelayAfterHealthCheckFailureMs;
        this.idleTimeBeforeHealthCheckMs = idleTimeBeforeHealthCheckMs;
        this.maxIdleTimeMs = maxIdleTimeMs;
        this.memberFactory = memberFactory;
        this.scheduler = scheduler;// schedules retries
    }

    private Single<Member2<T>> createMembers() {
        return new MembersSingle<T>(this);
    }

    @Override
    public Single<Member2<T>> member() {
        while (true) {
            Single<Member2<T>> m = member.get();
            if (m != null)
                return m;
            else {
                m = createMembers();
                if (member.compareAndSet(null, m)) {
                    return m;
                }
            }
        }
    }

    @Override
    public void close() {
        List<Member2<T>> ms = list.getAndSet(null);
        if (ms != null) {
            for (Member2<T> m : ms) {
                m.shutdown();
            }
        }
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
        private MemberFactory2<T, NonBlockingPool2<T>> memberFactory;
        private Scheduler scheduler = Schedulers.computation();
        private long maxIdleTimeMs;

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
            return returnToPoolDelayAfterHealthCheckFailureMs(unit.toMillis(value));
        }

        public Builder<T> memberFactory(MemberFactory2<T, NonBlockingPool2<T>> memberFactory) {
            Preconditions.checkNotNull(memberFactory);
            this.memberFactory = memberFactory;
            return this;
        }

        public Builder<T> scheduler(Scheduler scheduler) {
            Preconditions.checkNotNull(scheduler);
            this.scheduler = scheduler;
            return this;
        }

        public NonBlockingPool2<T> build() {
            return new NonBlockingPool2<T>(factory, healthy, disposer, maxSize,
                    returnToPoolDelayAfterHealthCheckFailureMs, idleTimeBeforeHealthCheckMs, maxIdleTimeMs,
                    memberFactory, scheduler);
        }
    }

}
