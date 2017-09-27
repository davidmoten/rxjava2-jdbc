package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

public final class TransactedReturnGeneratedKeysBuilder implements GetterTx {

    private final TransactedUpdateBuilder update;
    private final Database db;

    TransactedReturnGeneratedKeysBuilder(TransactedUpdateBuilder update, Database db) {
        this.update = update;
        this.db = db;
    }

    /**
     * Transforms the results using the given function.
     *
     * @param mapper
     *            maps the query results to an object
     * @return the results of the query as an Observable
     */
    @Override
    public <T> Flowable<Tx<T>> get(@Nonnull ResultSetMapper<? extends T> mapper) {
        Preconditions.checkNotNull(mapper, "mapper cannot be null");
        return Flowable.defer(() -> {
            AtomicReference<Connection> connection = new AtomicReference<Connection>();
            Flowable<T> o = Update.<T>createReturnGeneratedKeys( //
                    update.updateBuilder.connections //
                            .map(c -> Util.toTransactedConnection(connection, c)),
                    update.parameterGroupsToFlowable(), update.updateBuilder.sql, mapper, false);
            return o.materialize() //
                    .flatMap(n -> Tx.toTx(n, connection.get(), db)) //
                    .doOnNext(tx -> {
                        if (tx.isComplete()) {
                            ((TxImpl<T>) tx).connection().commit();
                        }
                    });
        });
    }

    public ValuesOnly valuesOnly() {
        return new ValuesOnly(this);
    }

    public static final class ValuesOnly implements Getter {

        private final TransactedReturnGeneratedKeysBuilder builder;

        public ValuesOnly(TransactedReturnGeneratedKeysBuilder builder) {
            this.builder = builder;
        }

        @Override
        public <T> Flowable<T> get(ResultSetMapper<? extends T> function) {
            return builder.get(function).flatMap(Tx.flattenToValuesOnly());
        }

    }

}
