package org.davidmoten.rx.jdbc;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
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

    public static <T> Function<Tx<T>, Flowable<T>> flattenToValuesOnly() {
        return tx -> {
            if (tx.isValue()) {
                return Flowable.just(tx.value());
            } else {
                return Flowable.empty();
            }
        };
    }
}