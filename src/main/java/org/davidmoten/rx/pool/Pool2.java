package org.davidmoten.rx.pool;

import io.reactivex.Single;

public interface Pool2<T> extends AutoCloseable {

    Single<Member2<T>> member();

}