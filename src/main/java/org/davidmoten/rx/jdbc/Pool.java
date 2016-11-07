package org.davidmoten.rx.jdbc;

import java.util.concurrent.Callable;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.subjects.PublishSubject;

public final class Pool<T> {

    private final Flowable<Member<T>> members;

    public Pool(Callable<T> factory, Predicate<T> healthy, Consumer<T> disposer, int maxSize, long retryDelayMs) {
        PublishSubject<Member<T>> subject = PublishSubject.create();
        Flowable<Member<T>> cachedMembers = Flowable //
                .range(1, maxSize) //
                .map(n -> new Member<T>(subject, factory, healthy, disposer, retryDelayMs)).cache();
        this.members = subject.toFlowable(BackpressureStrategy.BUFFER) //
                .mergeWith(cachedMembers) //
                // delay errors, maxConcurrent = 1 (don't request more than needed)
                .flatMap(member -> member.checkout().toFlowable(), true, 1);
    }

    public Flowable<Member<T>> members() {
        return members;
    }

}
