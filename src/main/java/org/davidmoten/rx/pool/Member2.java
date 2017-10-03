package org.davidmoten.rx.pool;

public interface Member2<T> {

    void checkin();

    void shutdown();

    T value();
}
