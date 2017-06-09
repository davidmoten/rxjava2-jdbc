package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.Single;

public final class TransactedUpdateBuilder implements DependsOn<TransactedUpdateBuilder> {

    private static final Logger log = LoggerFactory.getLogger(TransactedUpdateBuilder.class);

    final UpdateBuilder updateBuilder;
    private final Database db;
    private boolean valuesOnly;

    TransactedUpdateBuilder(UpdateBuilder b, Database db) {
        this.updateBuilder = b;
        this.db = db;
    }

    public TransactedUpdateBuilder parameterStream(Flowable<?> values) {
        updateBuilder.parameterStream(values);
        return this;
    }

    public TransactedUpdateBuilder parameterListStream(Flowable<List<?>> valueLists) {
        updateBuilder.parameterListStream(valueLists);
        return this;
    }

    public TransactedUpdateBuilder parameterList(List<Object> values) {
        updateBuilder.parameterList(values);
        return this;
    }

    public TransactedUpdateBuilder parameterList(Object... values) {
        updateBuilder.parameterList(values);
        return this;
    }

    public TransactedUpdateBuilder parameter(String name, Object value) {
        updateBuilder.parameter(name, value);
        return this;
    }

    public TransactedUpdateBuilder parameter(Object value) {
        return parameters(value);
    }

    public TransactedUpdateBuilder parameters(Object... values) {
        updateBuilder.parameters(values);
        return this;
    }

    @Override
    public TransactedUpdateBuilder dependsOn(Flowable<?> dependency) {
        updateBuilder.dependsOn(dependency);
        return this;
    }

    public TransactedUpdateBuilder batchSize(int batchSize) {
        updateBuilder.batchSize(batchSize);
        return this;
    }

    /**
     * Returns a builder used to specify how to process the generated keys
     * {@link ResultSet}. Not all jdbc drivers support this functionality and
     * some have limitations in their support (h2 for instance only returns the
     * last generated key when multiple inserts happen in the one statement).
     * 
     * @return a builder used to specify how to process the generated keys
     *         ResultSet
     */
    public TransactedReturnGeneratedKeysBuilder returnGeneratedKeys() {
        return new TransactedReturnGeneratedKeysBuilder(this, db);
    }

    public TransactedUpdateBuilder transactedValuesOnly() {
        this.valuesOnly = true;
        return this;
    }

    public TransactedUpdateBuilderValuesOnly valuesOnly() {
        return new TransactedUpdateBuilderValuesOnly(this, db);
    }

    public static final class TransactedUpdateBuilderValuesOnly {
        private final TransactedUpdateBuilder b;
        private final Database db;

        TransactedUpdateBuilderValuesOnly(TransactedUpdateBuilder b, Database db) {
            this.b = b;
            this.db = db;
        }

        // TODO add other methods e.g. parameter setting methods? Lots of
        // copy-and-paste not attractive here so may accept restricting
        // functionality once valuesOnly() called

        public Flowable<Integer> counts() {
            return createFlowable(b.updateBuilder, db) //
                    .flatMap(Tx.flattenToValuesOnly());
        }
    }

    public Flowable<Tx<Integer>> counts() {
        Flowable<Tx<Integer>> o = createFlowable(updateBuilder, db);
        if (valuesOnly) {
            return o.filter(tx -> tx.isValue());
        } else {
            return o;
        }
    }

    public Flowable<Integer> countsOnly() {
        return valuesOnly().counts();
    }

    @SuppressWarnings("unchecked")
    public Single<Tx<?>> tx() {
        return (Single<Tx<?>>) (Single<?>) createFlowable(updateBuilder, db) //
                .lastOrError();
    }

    private static Flowable<Tx<Integer>> createFlowable(UpdateBuilder ub, Database db) {
        return Flowable.defer(() -> {
            log.debug("creating deferred flowable");
            AtomicReference<Connection> connection = new AtomicReference<Connection>();
            AtomicReference<Tx<Integer>> t = new AtomicReference<>();
            return ub.startWithDependency( //
                    Update.create(
                            ub.connections //
                                    .firstOrError() //
                                    .map(c -> Util.toTransactedConnection(connection, c)), //
                            ub.parameterGroupsToFlowable(), //
                            ub.sql, //
                            ub.batchSize, //
                            false) //
                            .flatMap(n -> Tx.toTx(n, connection.get(), db)) //
                            .doOnNext(tx -> {
                                if (tx.isComplete()) {
                                    t.set(tx);
                                }
                            }) //
            );
        });
    }

    public Flowable<List<Object>> parameterGroupsToFlowable() {
        return updateBuilder.parameterGroupsToFlowable();
    }
}
