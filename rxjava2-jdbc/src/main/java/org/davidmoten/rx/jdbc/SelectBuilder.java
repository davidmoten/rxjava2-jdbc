package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.List;

import javax.annotation.Nonnull;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.Single;

public final class SelectBuilder extends ParametersBuilder<SelectBuilder>
        implements Getter, DependsOn<SelectBuilder> {

    final String sql;
    final Single<Connection> connection;
    private final Database db;

    int fetchSize = 0; // default
    int queryTimeoutSec = Util.QUERY_TIMEOUT_NOT_SET; //default
    private Flowable<?> dependsOn;

    SelectBuilder(String sql, Single<Connection> connection, Database db) {
        super(sql);
        Preconditions.checkNotNull(sql);
        Preconditions.checkNotNull(connection);
        this.sql = sql;
        this.connection = connection;
        this.db = db;
    }

    /**
     * Sets the fetchSize for the JDBC statement. If 0 then fetchSize is not set and
     * the default fetchSize for the JDBC driver you are using will be used.
     * 
     * @param size
     *            sets the fetchSize or chooses default value if 0
     * @return this
     */
    public SelectBuilder fetchSize(int size) {
        Preconditions.checkArgument(size >= 0);
        this.fetchSize = size;
        return this;
    }

    public SelectBuilder queryTimeoutSec(int timeoutSec) {
        Preconditions.checkArgument(timeoutSec >= 0);
        this.queryTimeoutSec = timeoutSec;
        return this;
    }

    public TransactedSelectBuilder transacted() {
        return new TransactedSelectBuilder(this, db);
    }

    public TransactedSelectBuilder transactedValuesOnly() {
        return transacted().transactedValuesOnly();
    }

    @Override
    public <T> Flowable<T> get(@Nonnull ResultSetMapper<? extends T> mapper) {
        Preconditions.checkNotNull(mapper, "mapper cannot be null");
        Flowable<List<Object>> pg = super.parameterGroupsToFlowable();
        Flowable<T> f = Select.<T>create(connection, pg, sql, fetchSize, mapper, true, queryTimeoutSec);
        if (dependsOn != null) {
            return dependsOn.ignoreElements().andThen(f);
        } else {
            return f;
        }
    }

    @Override
    public SelectBuilder dependsOn(@Nonnull Flowable<?> flowable) {
        Preconditions.checkArgument(dependsOn == null, "can only set dependsOn once");
        Preconditions.checkNotNull(flowable);
        dependsOn = flowable;
        return this;
    }

}
