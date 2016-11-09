package org.davidmoten.rx.jdbc.pool;

import io.reactivex.Flowable;

public interface Pool<T> {

    Flowable<Member<T>> members();

    void shutdown();

}