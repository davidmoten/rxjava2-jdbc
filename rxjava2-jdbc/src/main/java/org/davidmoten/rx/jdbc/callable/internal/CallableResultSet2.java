package org.davidmoten.rx.jdbc.callable.internal;

import java.util.List;

import io.reactivex.Flowable;

public final class CallableResultSet2<T1, T2> {

    private final List<Object> outs;
    private final Flowable<T1> results1;
    private final Flowable<T2> results2;

    public CallableResultSet2(List<Object> outs, Flowable<T1> results1, Flowable<T2> results2) {
        this.outs = outs;
        this.results1 = results1;
        this.results2 = results2;
    }

    public Flowable<T1> results1() {
        return results1;
    }

    public Flowable<T2> results2() {
        return results2;
    }

    public List<Object> outs() {
        return outs;
    }
}