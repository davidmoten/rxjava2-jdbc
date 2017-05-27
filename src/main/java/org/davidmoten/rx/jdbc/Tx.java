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
    
    TransactedSelectBuilder select(String sql);
    
    public static <T> Predicate<Tx<T>> valuesOnly() {
        return tx -> tx.isValue();
    }

    public static <T> Function<Tx<T>, Flowable<T>> flattenToValuesOnly() {
        return tx -> {
            if (tx.isValue()) {
                return Flowable.just(tx.value());
            } else if (tx.isComplete()) {
                return Flowable.empty();
            } else {
                return Flowable.error(tx.throwable());
            }
        };
    }

    public static <T> Function<Tx<T>, T> toValue() {
        return tx -> tx.value();
    }
}