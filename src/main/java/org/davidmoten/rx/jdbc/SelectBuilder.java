package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

public final class SelectBuilder implements Getter {

    final String sql;
    final Flowable<Connection> connections;
    private final Database db;

    private final ParametersBuilder parameters;

    int fetchSize = 0; // default

    public SelectBuilder(String sql, Flowable<Connection> connections, Database db) {
        Preconditions.checkNotNull(sql);
        Preconditions.checkNotNull(connections);
        this.sql = sql;
        this.connections = connections;
        this.parameters = new ParametersBuilder(sql);
        this.db = db;
    }

    public SelectBuilder parameterStream(Flowable<?> values) {
        parameters.parameterStream(values);
        return this;
    }

    public SelectBuilder parameterListStream(Flowable<List<?>> valueLists) {
        parameters.parameterListStream(valueLists);
        return this;
    }

    public SelectBuilder parameterList(List<Object> values) {
        parameters.parameterList(values);
        return this;
    }

    public SelectBuilder parameterList(Object... values) {
        parameters.parameterList(values);
        return this;
    }

    public SelectBuilder parameter(String name, Object value) {
        parameters.parameter(name, value);
        return this;
    }

    public SelectBuilder parameters(Object... values) {
        parameters.parameters(values);
        return this;
    }
    
    Flowable<List<Object>> parameterGroupsToFlowable() {
        return parameters.parameterGroupsToFlowable();
    }

    public SelectBuilder fetchSize(int size) {
        Preconditions.checkArgument(size >= 0);
        this.fetchSize = size;
        return this;
    }

    public TransactedSelectBuilder transacted() {
        return new TransactedSelectBuilder(this, db);
    }

    public TransactedSelectBuilder transactedValuesOnly() {
        return transacted().transactedValuesOnly();
    }

    @Override
    public <T> Flowable<T> get(ResultSetMapper<? extends T> function) {
        Flowable<List<Object>> pg = parameters.parameterGroupsToFlowable();
        return Select.<T> create(connections.firstOrError(), pg, sql, fetchSize, function);
    }

}
