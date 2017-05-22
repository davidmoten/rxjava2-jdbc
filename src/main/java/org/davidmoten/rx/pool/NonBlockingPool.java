package org.davidmoten.rx.pool;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    final long retryDelayMs;
    final MemberFactory<T, NonBlockingPool<T>> memberFactory;
    final Scheduler scheduler;

    private final Flowable<Member<T>> members;


    private NonBlockingPool(Callable<T> factory, Predicate<T> healthy, Consumer<T> disposer, int maxSize,
            long retryDelayMs, long idleTimeBeforeHealthCheckMs, long maxIdleTimeMs,
            MemberFactory<T, NonBlockingPool<T>> memberFactory, Scheduler scheduler) {
        Preconditions.checkNotNull(factory);
        Preconditions.checkNotNull(healthy);
        Preconditions.checkNotNull(disposer);
        Preconditions.checkArgument(maxSize > 0);
        Preconditions.checkArgument(retryDelayMs >= 0);
        Preconditions.checkNotNull(memberFactory);
        Preconditions.checkNotNull(scheduler);
        this.factory = factory;
        // TODO use healthy
        this.healthy = healthy;
        this.disposer = disposer;
        this.maxSize = maxSize;
        this.retryDelayMs = retryDelayMs;
        this.idleTimeBeforeHealthCheckMs = idleTimeBeforeHealthCheckMs;
        this.maxIdleTimeMs = maxIdleTimeMs;
        this.memberFactory = memberFactory;
        this.scheduler = scheduler;
        this.subject = PublishSubject.create();

        AtomicReference<List<Member<T>>> list = new AtomicReference<>();
        Flowable<Member<T>> baseMembers = Flowable.defer(() -> {
            if (list.compareAndSet(null, Collections.emptyList())) {
                List<Member<T>> m = IntStream.range(1, maxSize + 1)
                        .mapToObj(n -> memberFactory.create(NonBlockingPool.this)) //
                        .collect(Collectors.toList());
                list.set(m);
                log.debug("created pool of size={}", maxSize);
            }
            return Flowable.fromIterable(list.get()) //
                    .doOnRequest(n -> log.debug("requested={}", n));
        });

        Flowable<Member<T>> returnedMembers = subject //
                .toSerialized() //
                .toFlowable(BackpressureStrategy.BUFFER);

        this.members = returnedMembers //
                .doOnNext(m -> log.debug("returned member reentering")) //
                .mergeWith(baseMembers) //
                .doOnNext(x -> log.debug("member={}", x)) //
                .<Member<T>>flatMap(member -> member.checkout().toFlowable(), false, 1) //
                .doOnNext(x -> log.debug("checked out member={}", x));
    }

    @Override
    public Flowable<Member<T>> members() {
        return members;
    }

    @Override
    public void close() {
        // TODO
    }

    public static <T> Builder<T> builder() {
        return new Builder<T>();
    }

    public static <T> Builder<T> factory(Callable<T> factory) {
        return new Builder<T>().factory(factory);
    }

    public static class Builder<T> {

        private Callable<T> factory;
        private Predicate<T> healthy = x -> true;
        private long idleTimeBeforeHealthCheckMs = 30;
        private Consumer<T> disposer;
        private int maxSize = 10;
        private long retryDelayMs = 30000;
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

        public Builder<T> retryDelayMs(long retryDelayMs) {
            Preconditions.checkArgument(retryDelayMs >= 0);
            this.retryDelayMs = retryDelayMs;
            return this;
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
            return new NonBlockingPool<T>(factory, healthy, disposer, maxSize, retryDelayMs,
                    idleTimeBeforeHealthCheckMs, maxIdleTimeMs, memberFactory, scheduler);
        }
    }

}
