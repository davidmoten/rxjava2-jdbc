package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Flowable;

public final class TransactedSelectAutomappedBuilder<T> {

    private final SelectAutomappedBuilder<T> selectBuilder;
    private final Database db;

    // mutable
    private boolean valuesOnly = false;

    TransactedSelectAutomappedBuilder(SelectAutomappedBuilder<T> selectBuilder, Database db) {
        this.selectBuilder = selectBuilder;
        this.db = db;
    }

    public TransactedSelectAutomappedBuilder<T> parameters(Flowable<List<Object>> parameters) {
        selectBuilder.parameters(parameters);
        return this;
    }

    public TransactedSelectAutomappedBuilder<T> parameterList(List<Object> values) {
        selectBuilder.parameterList(values);
        return this;
    }

    public TransactedSelectAutomappedBuilder<T> parameterList(Object... values) {
        selectBuilder.parameterList(values);
        return this;
    }

    public TransactedSelectAutomappedBuilder<T> parameter(String name, Object value) {
        selectBuilder.parameter(name, value);
        return this;
    }

    public TransactedSelectAutomappedBuilder<T> parameters(Object... values) {
        selectBuilder.parameters(values);
        return this;
    }

    public TransactedSelectAutomappedBuilder<T> parameter(Object value) {
        selectBuilder.parameters(value);
        return this;
    }

    public TransactedSelectAutomappedBuilder<T> transactedValuesOnly() {
        this.valuesOnly = true;
        return this;
    }

    public TransactedSelectAutomappedBuilderValuesOnly<T> valuesOnly() {
        return new TransactedSelectAutomappedBuilderValuesOnly<T>(this, db);
    }

    public static final class TransactedSelectAutomappedBuilderValuesOnly<T> {
        private final TransactedSelectAutomappedBuilder<T> b;
        private final Database db;

        TransactedSelectAutomappedBuilderValuesOnly(TransactedSelectAutomappedBuilder<T> b, Database db) {
            this.b = b;
            this.db = db;
        }

        public Flowable<T> get() {
            return createFlowable(b.selectBuilder, db) //
                    .flatMap(Tx.flattenToValuesOnly());
        }
    }

    public Flowable<Tx<T>> get() {
        Flowable<Tx<T>> o = createFlowable(selectBuilder, db);
        if (valuesOnly) {
            return o.filter(tx -> tx.isValue());
        } else {
            return o;
        }
    }

    private static <T> Flowable<Tx<T>> createFlowable(SelectAutomappedBuilder<T> sb, Database db) {
        return Flowable.defer(() -> {
            AtomicReference<Connection> connection = new AtomicReference<Connection>();
            return Select
                    .create(sb.selectBuilder.connections.firstOrError() //
                            .map(c -> Util.toTransactedConnection(connection, c)), //
                            sb.selectBuilder.parameterGroupsToFlowable(), //
                            sb.selectBuilder.sql, //
                            sb.selectBuilder.fetchSize, //
                            rs -> Util.mapObject(rs, sb.cls, 1)) //
                    .materialize() //
                    .flatMap(n -> Tx.toTx(n, connection.get(), db)) //
                    .doOnNext(tx -> {
                        if (tx.isComplete()) {
                            ((TxImpl<T>) tx).connection().commit();
                        }
                    });
        });
    }

}
