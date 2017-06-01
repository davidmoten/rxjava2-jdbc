package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;

public final class SelectBuilder extends ParametersBuilder<SelectBuilder> implements Getter {

    final String sql;
    final Flowable<Connection> connections;
    private final Database db;

    int fetchSize = 0; // default
    private Flowable<?> dependsOn;

    public SelectBuilder(String sql, Flowable<Connection> connections, Database db) {
        super(sql);
        Preconditions.checkNotNull(sql);
        Preconditions.checkNotNull(connections);
        this.sql = sql;
        this.connections = connections;
        this.db = db;
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
        Flowable<List<Object>> pg = super.parameterGroupsToFlowable();
        Flowable<T> f = Select.<T> create(connections.firstOrError(), pg, sql, fetchSize, function);
        if (dependsOn != null) {
            return dependsOn.ignoreElements().andThen(f);
        } else {
            return f;
        }
    }

    public Getter dependsOn(Flowable<?> flowable) {
        Preconditions.checkArgument(dependsOn == null, "can only set dependsOn once");
        Preconditions.checkNotNull(flowable);
        dependsOn = flowable;
        return this;
    }

    public Getter dependsOn(Observable<?> observable) {
        return dependsOn(observable.ignoreElements().toFlowable());
    }

    public Getter dependsOn(Single<?> single) {
        return dependsOn(single.toFlowable());
    }

    public Getter dependsOn(Completable completable) {
        return dependsOn(completable.toFlowable());
    }

}
