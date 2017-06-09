package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

public final class SelectBuilder extends ParametersBuilder<SelectBuilder>
        implements Getter, DependsOn<SelectBuilder> {

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

    /**
     * Sets the fetchSize for the JDBC statement. If 0 then fetchSize is not set
     * and the default fetchSize for the JDBC driver you are using will be used.
     * 
     * @param size sets the fetchSize or chooses default value if 0
     * @return this
     */
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
        Flowable<T> f = Select.<T> create(connections.firstOrError(), pg, sql, fetchSize, function,
                true);
        if (dependsOn != null) {
            return dependsOn.ignoreElements().andThen(f);
        } else {
            return f;
        }
    }

    @Override
    public SelectBuilder dependsOn(Flowable<?> flowable) {
        Preconditions.checkArgument(dependsOn == null, "can only set dependsOn once");
        Preconditions.checkNotNull(flowable);
        dependsOn = flowable;
        return this;
    }

}
