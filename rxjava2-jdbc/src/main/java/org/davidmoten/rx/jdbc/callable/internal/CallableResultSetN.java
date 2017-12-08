package org.davidmoten.rx.jdbc.callable.internal;

import java.util.List;

import io.reactivex.Flowable;

public final class CallableResultSetN {

    private final List<Object> outs;
    private final List<Flowable<?>> flowables;

    public CallableResultSetN(List<Object> outs, List<Flowable<?>> flowables) {
        this.outs = outs;
        this.flowables = flowables;
    }

    public List<Flowable<?>> results() {
        return flowables;
    }

    public Flowable<?> results(int index) {
        return flowables.get(index);
    }

    public List<Object> outs() {
        return outs;
    }
}