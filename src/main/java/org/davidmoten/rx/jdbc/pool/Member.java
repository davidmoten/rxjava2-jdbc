package org.davidmoten.rx.jdbc.pool;

import io.reactivex.Maybe;

public interface Member<T> {

    Maybe<Member<T>> checkout();

    void checkin();

    T value();

}