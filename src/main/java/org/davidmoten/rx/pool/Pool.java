package org.davidmoten.rx.pool;

import io.reactivex.Single;

public interface Pool<T> extends AutoCloseable {

    Single<Member<T>> member();

}