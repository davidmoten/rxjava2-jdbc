package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.List;

import org.davidmoten.rx.jdbc.annotations.Query;
import org.davidmoten.rx.jdbc.exceptions.QueryAnnotationMissingException;

import io.reactivex.Flowable;
import io.reactivex.Single;

public final class SelectAutomappedBuilder<T> {

    final SelectBuilder selectBuilder;

    final Class<T> cls;

    private final Database db;

    SelectAutomappedBuilder(Class<T> cls, Flowable<Connection> connections, Database db) {
        this.selectBuilder = new SelectBuilder(getSql(cls), connections, db);
        this.cls = cls;
        this.db = db;
    }

    private static String getSql(Class<?> cls) {
        Query q = cls.getDeclaredAnnotation(Query.class);
        if (q == null) {
            throw new QueryAnnotationMissingException(
                    "the sql for the automapped interface should be specified in a Query annotation on the interface");
        }
        return q.value();
    }

    public TransactedSelectAutomappedBuilder<T> transacted() {
        return new TransactedSelectAutomappedBuilder<T>(this, db);
    }

    public TransactedSelectAutomappedBuilder<T> transactedValuesOnly() {
        return transacted().transactedValuesOnly();
    }

    public Flowable<T> get() {
        return selectBuilder.autoMap(cls);
    }

    public SelectAutomappedBuilder<T> parameterStream(Flowable<?> values) {
        selectBuilder.parameterStream(values);
        return this;
    }

    public SelectAutomappedBuilder<T> fetchSize(int size) {
        selectBuilder.fetchSize(size);
        return this;
    }

    public SelectAutomappedBuilder<T> parameterListStream(Flowable<List<?>> valueLists) {
        selectBuilder.parameterListStream(valueLists);
        return this;
    }

    public SelectAutomappedBuilder<T> parameterList(List<Object> values) {
        selectBuilder.parameterList(values);
        return this;
    }

    public SelectAutomappedBuilder<T> parameterList(Object... values) {
        selectBuilder.parameterList(values);
        return this;
    }

    public SelectAutomappedBuilder<T> parameter(String name, Object value) {
        selectBuilder.parameter(name, value);
        return this;
    }

    public SelectAutomappedBuilder<T> parameters(Object... values) {
        selectBuilder.parameters(values);
        return this;
    }

    public SelectAutomappedBuilder<T> parameterClob(String value) {
        selectBuilder.parameterClob(value);
        return this;
    }

    public SelectAutomappedBuilder<T> parameterBlob(byte[] bytes) {
        selectBuilder.parameterBlob(bytes);
        return this;
    }

    public Single<Long> count() {
        return selectBuilder.count();
    }

}
