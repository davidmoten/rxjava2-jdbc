package org.davidmoten.rx.jdbc;

import javax.annotation.Nonnull;

import io.reactivex.Flowable;

public final class ReturnGeneratedKeysBuilder implements Getter {

    private final UpdateBuilder update;

    ReturnGeneratedKeysBuilder(@Nonnull UpdateBuilder update) {
        this.update = update;
    }

    /**
     * Transforms the results using the given function.
     *
     * @param function
     * @return the results of the query as an Observable
     */
    @Override
    public <T> Flowable<T> get(@Nonnull ResultSetMapper<? extends T> function) {
        return update.startWithDependency(
                Update.<T> createReturnGeneratedKeys(update.connections.firstOrError(),
                        update.parameterGroupsToFlowable(), update.sql, function, true));

    }

}
