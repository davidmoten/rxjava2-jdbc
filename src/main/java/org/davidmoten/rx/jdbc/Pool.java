package org.davidmoten.rx.jdbc;

import java.util.concurrent.Callable;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.PublishSubject;

public class Pool<T> {

    private final Flowable<Member<T>> members;

    public Pool(Callable<T> factory, int maxSize) {
        PublishSubject<Member<T>> subject = PublishSubject.create();
        this.members = Flowable //
                .range(1, maxSize) //
                .map(n -> new Member<T>(factory.call(), subject)) //
                .mergeWith(subject.toFlowable(BackpressureStrategy.BUFFER)) //
                .share() //
                .doOnNext(m -> System.out.println("candidate: " + m)) //
                .filter(member -> member.checkout()) //
                .doOnNext(m -> System.out.println("checked out: " + m));
        // need at least one subscriber otherwise if subscribers got to zero
        // then up again we will create another maxSize members
        members.test(0);
    }

    public Flowable<Member<T>> members() {
        return members;
    }

}
