package org.davidmoten.rx.pool;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.davidmoten.rx.jdbc.pool.PoolClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;

public final class NonBlockingPool<T> implements Pool<T> {

    private static final Logger log = LoggerFactory.getLogger(NonBlockingPool.class);

    final PublishSubject<Member<T>> subject;
    final Callable<T> factory;
    final Predicate<T> healthy;
    final long idleTimeBeforeHealthCheckMs;
    final Consumer<T> disposer;
    final int maxSize;
    final long maxIdleTimeMs;
    final long returnToPoolDelayAfterHealthCheckFailureMs;
    final MemberFactory<T, NonBlockingPool<T>> memberFactory;
    final Scheduler scheduler;

    private final AtomicReference<Flowable<Member<T>>> members = new AtomicReference<>();
    private final AtomicReference<List<Member<T>>> list = new AtomicReference<>(
            Collections.emptyList());

    private NonBlockingPool(Callable<T> factory, Predicate<T> healthy, Consumer<T> disposer,
            int maxSize, long returnToPoolDelayAfterHealthCheckFailureMs,
            long idleTimeBeforeHealthCheckMs, long maxIdleTimeMs,
            MemberFactory<T, NonBlockingPool<T>> memberFactory, Scheduler scheduler) {
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
        this.scheduler = scheduler;//schedules retries
        this.subject = PublishSubject.create();
    }

    private Flowable<Member<T>> createMembers() {
        List<Member<T>> ms = IntStream.range(1, maxSize + 1)
                .mapToObj(n -> memberFactory.create(NonBlockingPool.this)) //
                .collect(Collectors.toList());

        Flowable<Member<T>> baseMembers = Flowable.fromIterable(ms) //
                .doOnRequest(n -> log.debug("requested={}", n));

        Flowable<Member<T>> returnedMembers = subject //
                .toSerialized() //
                .toFlowable(BackpressureStrategy.BUFFER);

        // use CAS loop to handle a race with close
        while (true) {
            List<Member<T>> l = list.get();
            if (l == null) {
                return Flowable.error(new PoolClosedException());
            } else if (list.compareAndSet(Collections.emptyList(), ms)) {
                return returnedMembers //
                        .doOnNext(m -> log.debug("returned member reentering")) //
                        .mergeWith(baseMembers) //
                        .doOnNext(x -> log.debug("member={}", x)) //
                        .<Member<T>> flatMap(member -> member.checkout().toFlowable(), false, 1) //
                        .doOnNext(x -> log.debug("checked out member={}", x))
                        //reduce stack
                        .observeOn(scheduler);
            }
        }
    }

    @Override
    public Flowable<Member<T>> members() {
        while (true) {
            Flowable<Member<T>> m = members.get();
            if (m != null)
                return m;
            else {
                m = createMembers();
                if (members.compareAndSet(null, m)) {
                    return m;
                }
            }
        }
    }

    @Override
    public void close() {
        List<Member<T>> ms = list.getAndSet(null);
        if (ms != null) {
            for (Member<T> m : ms) {
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
        private MemberFactory<T, NonBlockingPool<T>> memberFactory;
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

        public Builder<T> memberFactory(MemberFactory<T, NonBlockingPool<T>> memberFactory) {
            Preconditions.checkNotNull(memberFactory);
            this.memberFactory = memberFactory;
            return this;
        }

        public Builder<T> scheduler(Scheduler scheduler) {
            Preconditions.checkNotNull(scheduler);
            this.scheduler = scheduler;
            return this;
        }

        public NonBlockingPool<T> build() {
            return new NonBlockingPool<T>(factory, healthy, disposer, maxSize,
                    returnToPoolDelayAfterHealthCheckFailureMs, idleTimeBeforeHealthCheckMs,
                    maxIdleTimeMs, memberFactory, scheduler);
        }
    }

}
