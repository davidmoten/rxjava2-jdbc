package org.davidmoten.rx.jdbc;

import io.reactivex.Flowable;

public class SelectBuilder {

    private final String sql;

    private Flowable<Object> parameters = Flowable.empty();

    public SelectBuilder(String sql) {
        this.sql = sql;
    }

    public SelectBuilder parameters(Flowable<Object> parameters) {
        this.parameters = this.parameters.concatWith(parameters);
        return this;
    }

    public SelectBuilder parameter(Object value) {
        this.parameters = this.parameters.concatWith(Flowable.just(value));
        return this;
    }

    public SelectBuilder parameter(String name, Object value) {
        this.parameters = this.parameters.concatWith(Flowable.just(new Parameter(name, value)));
        return this;
    }

    public SelectBuilder parameters(Object... values) {
        return parameters(Flowable.fromArray(values));
    }

}
