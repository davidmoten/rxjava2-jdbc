package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Flowable;

public final class TransactedUpdateBuilder {

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

    public TransactedUpdateBuilder parameters(Object... values) {
        updateBuilder.parameters(values);
        return this;
    }

    /**
     * Appends a parameter to the parameter list for the query for a CLOB
     * parameter and handles null appropriately. If there are more parameters
     * than required for one execution of the query then more than one execution
     * of the query will occur.
     * 
     * @param value
     *            the string to insert in the CLOB column
     * @return this
     */
    public TransactedUpdateBuilder parameterClob(String value) {
        updateBuilder.parameterClob(value);
        return this;
    }

    /**
     * Appends a parameter to the parameter list for the query for a CLOB
     * parameter and handles null appropriately. If there are more parameters
     * than required for one execution of the query then more than one execution
     * of the query will occur.
     * 
     * @param value
     * @return this
     */
    public TransactedUpdateBuilder parameterBlob(byte[] bytes) {
        updateBuilder.parameterBlob(bytes);
        return this;
    }

    /**
     * Appends a dependency to the dependencies that have to complete their
     * emitting before the query is executed.
     * 
     * @param dependency
     *            dependency that must complete before the Flowable built by
     *            this subscribes.
     * @return this this
     */
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
    public ReturnGeneratedKeysBuilder returnGeneratedKeys() {
        return updateBuilder.returnGeneratedKeys();
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

    private static Flowable<Tx<Integer>> createFlowable(UpdateBuilder ub, Database db) {
        return Flowable.defer(() -> {
            AtomicReference<Connection> connection = new AtomicReference<Connection>();
            return Update
                    .create(ub.connections //
                            .firstOrError() //
                            .map(c -> Util.toTransactedConnection(connection, c)), //
                            ub.parameterGroupsToFlowable(), ub.sql, ub.batchSize)
                    .materialize() //
                    .flatMap(n -> Tx.toTx(n, connection.get(), db)) //
                    .doOnNext(tx -> {
                        if (tx.isComplete()) {
                            ((TxImpl<Integer>) tx).connection().commit();
                        }
                    });
        });
    }

    public Flowable<List<Object>> parameterGroupsToFlowable() {
        return updateBuilder.parameterGroupsToFlowable();
    }
}
