package org.davidmoten.rx.jdbc;

import io.reactivex.Flowable;

public final class ReturnGeneratedKeysBuilder {

    private final UpdateBuilder update;

    ReturnGeneratedKeysBuilder(UpdateBuilder update) {
        this.update = update;
    }

    /**
     * Transforms the results using the given function.
     *
     * @param function
     * @return the results of the query as an Observable
     */
    public <T> Flowable<T> get(ResultSetMapper<? extends T> function) {
        return Update.<T> createReturnGeneratedKeys(update.connections,
                update.parameterGroupsToFlowable(), update.sql, function);
    }

    public <T> Flowable<T> getAs(Class<T> cls) {
        return get(rs -> Util.mapObject(rs, cls, 1));
    }

}
