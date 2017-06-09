package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Flowable;

public final class TransactedReturnGeneratedKeysBuilder {

    private final TransactedUpdateBuilder update;
    private final Database db;

    TransactedReturnGeneratedKeysBuilder(TransactedUpdateBuilder update, Database db) {
        this.update = update;
        this.db = db;
    }

    /**
     * Transforms the results using the given function.
     *
     * @param function
     * @return the results of the query as an Observable
     */
    public <T> Flowable<Tx<T>> get(ResultSetMapper<? extends T> function) {
        return Flowable.defer(() -> {
            AtomicReference<Connection> connection = new AtomicReference<Connection>();
            Flowable<T> o = Update.<T>createReturnGeneratedKeys( //
                    update.updateBuilder.connections //
                            .firstOrError() //
                            .map(c -> Util.toTransactedConnection(connection, c)),
                    update.parameterGroupsToFlowable(), update.updateBuilder.sql, function, false);
            return o.materialize() //
                    .flatMap(n -> Tx.toTx(n, connection.get(), db)) //
                    .doOnNext(tx -> {
                        if (tx.isComplete()) {
                            ((TxImpl<T>) tx).connection().commit();
                        }
                    });
        });
    }

    public <T> Flowable<Tx<T>> getAs(Class<T> cls) {
        return get(rs -> Util.mapObject(rs, cls, 1));
    }
    
    public ValuesOnly valuesOnly() {
        return new ValuesOnly(this);
    }
    
    public static final class ValuesOnly {

        private final TransactedReturnGeneratedKeysBuilder builder;

        public ValuesOnly(TransactedReturnGeneratedKeysBuilder builder) {
            this.builder = builder;
        }
        public <T> Flowable<T> get(ResultSetMapper<? extends T> function) {
            return builder.get(function).flatMap(Tx.flattenToValuesOnly());
        }
        
        public <T> Flowable<T> getAs(Class<T> cls) {
            return builder.getAs(cls).flatMap(Tx.flattenToValuesOnly());
        }
        
    }

}
