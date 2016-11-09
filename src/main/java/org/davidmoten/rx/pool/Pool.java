package org.davidmoten.rx.pool;

import io.reactivex.Flowable;

public interface Pool<T> extends AutoCloseable {

    Flowable<Member<T>> members();

}