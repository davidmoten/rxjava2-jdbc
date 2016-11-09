package org.davidmoten.rx.jdbc.pool;

import java.util.concurrent.Callable;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.subjects.PublishSubject;

public final class NonBlockingPool<T> implements Pool<T> {

    private final Flowable<Member<T>> members;

    final PublishSubject<Member<T>> subject;
    final Callable<T> factory;
    final Predicate<T> healthy;
    final Consumer<T> disposer;
    final int maxSize;
    final long retryDelayMs;
    final MemberFactory<T, NonBlockingPool<T>> memberFactory;
    final Scheduler scheduler;

    public NonBlockingPool(Callable<T> factory, Predicate<T> healthy, Consumer<T> disposer,
            int maxSize, long retryDelayMs, MemberFactory<T, NonBlockingPool<T>> memberFactory,
            Scheduler scheduler) {
        this.factory = factory;
        this.healthy = healthy;
        this.disposer = disposer;
        this.maxSize = maxSize;
        this.retryDelayMs = retryDelayMs;
        this.memberFactory = memberFactory;
        this.scheduler = scheduler;
        this.subject = PublishSubject.create();
        Flowable<Member<T>> cachedMembers = Flowable //
                .range(1, maxSize) //
                .map(n -> memberFactory.create(NonBlockingPool.this)) //
                .cache();
        this.members = subject //
                .toFlowable(BackpressureStrategy.BUFFER) //
                .mergeWith(cachedMembers) //
                // delay errors, maxConcurrent = 1 (don't request more than
                // needed)
                .flatMap(member -> member.checkout().toFlowable(), true, 1);
    }

    @Override
    public Flowable<Member<T>> members() {
        return members;
    }

    @Override
    public void shutdown() {
        // TODO
    }

}
