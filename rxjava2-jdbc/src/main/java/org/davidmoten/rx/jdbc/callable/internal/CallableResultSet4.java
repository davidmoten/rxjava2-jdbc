package org.davidmoten.rx.jdbc.callable.internal;

import java.util.List;

import io.reactivex.Flowable;

public final class CallableResultSet4<T1, T2, T3, T4> {

    private final List<Object> outs;
    private final Flowable<T1> results1;
    private final Flowable<T2> results2;
    private final Flowable<T3> results3;
    private final Flowable<T4> results4;

    public CallableResultSet4(List<Object> outs, Flowable<T1> query1, Flowable<T2> query2, Flowable<T3> query3,
            Flowable<T4> query4) {
        this.outs = outs;
        this.results1 = query1;
        this.results2 = query2;
        this.results3 = query3;
        this.results4 = query4;
    }

    public Flowable<T1> results1() {
        return results1;
    }

    public Flowable<T2> results2() {
        return results2;
    }

    public Flowable<T3> results3() {
        return results3;
    }

    public Flowable<T4> results4() {
        return results4;
    }

    public List<Object> outs() {
        return outs;
    }
}