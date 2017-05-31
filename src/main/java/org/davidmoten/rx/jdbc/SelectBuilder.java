package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

public final class SelectBuilder extends ParametersBuilder<SelectBuilder> implements Getter {

    final String sql;
    final Flowable<Connection> connections;
    private final Database db;

    int fetchSize = 0; // default

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
        return Select.<T> create(connections.firstOrError(), pg, sql, fetchSize, function);
    }

}
