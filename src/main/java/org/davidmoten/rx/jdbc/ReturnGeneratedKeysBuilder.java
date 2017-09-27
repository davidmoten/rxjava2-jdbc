package org.davidmoten.rx.jdbc;

import javax.annotation.Nonnull;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

public final class ReturnGeneratedKeysBuilder implements Getter {

    private final UpdateBuilder update;

    ReturnGeneratedKeysBuilder(@Nonnull UpdateBuilder update) {
        this.update = update;
    }

    /**
     * Transforms the results using the given function.
     *
     * @param mapper maps the query result to an object 
     * @return the results of the query as an Observable
     */
    @Override
    public <T> Flowable<T> get(@Nonnull ResultSetMapper<? extends T> mapper) {
        Preconditions.checkNotNull(mapper, "mapper cannot be null");
        return update.startWithDependency(
                Update.<T> createReturnGeneratedKeys(update.connections,
                        update.parameterGroupsToFlowable(), update.sql, mapper, true));

    }

}
