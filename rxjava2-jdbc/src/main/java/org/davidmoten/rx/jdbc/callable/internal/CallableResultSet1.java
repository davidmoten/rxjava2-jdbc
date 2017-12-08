package org.davidmoten.rx.jdbc.callable.internal;

import java.util.List;

import io.reactivex.Flowable;

public final class CallableResultSet1<T1> {

    private final List<Object> outs;
    private final Flowable<T1> results;

    public CallableResultSet1(List<Object> outs, Flowable<T1> results) {
        this.outs = outs;
        this.results = results;
    }

    public Flowable<T1> results() {
        return results;
    }

    public List<Object> outs() {
        return outs;
    }

}