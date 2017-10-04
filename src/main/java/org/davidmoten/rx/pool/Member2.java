package org.davidmoten.rx.pool;

public interface Member2<T> {

    void checkin();

    /**
     * This method should not throw. Feel free to add logging so that you are aware
     * of a problem with disposal.
     */
    void disposeValue();

    T value();
}
