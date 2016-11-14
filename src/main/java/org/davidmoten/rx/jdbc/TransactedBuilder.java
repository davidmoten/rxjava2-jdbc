package org.davidmoten.rx.jdbc;

import java.util.concurrent.Callable;

public class TransactedBuilder<T> {

    private final Callable<Tx<T>> txFactory;

    public TransactedBuilder(Callable<Tx<T>> txFactory) {
        this.txFactory = txFactory;
    }

}
