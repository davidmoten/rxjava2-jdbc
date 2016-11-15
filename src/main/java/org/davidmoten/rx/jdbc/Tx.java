package org.davidmoten.rx.jdbc;

import io.reactivex.functions.Predicate;

public interface Tx<T> {

    boolean isValue();

    boolean isComplete();

    boolean isError();

    T value();

    Throwable throwable();

    public static <T> Predicate<Tx<T>> valuesOnly() {
        return tx -> tx.isValue();
    }
}