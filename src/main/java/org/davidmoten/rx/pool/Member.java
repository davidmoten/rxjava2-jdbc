package org.davidmoten.rx.pool;

import io.reactivex.Maybe;

public interface Member<T> extends AutoCloseable {

    Maybe<? extends Member<T>> checkout();

    void checkin();

    /**
     * Should only be called if the Member has been checked out (managed by the
     * Pool instance).
     * 
     * @return the value of the pooled member
     */
    T value();

    void shutdown();
}