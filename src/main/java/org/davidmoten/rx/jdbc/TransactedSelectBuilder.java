package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Flowable;

public final class TransactedSelectBuilder {

    private final SelectBuilder selectBuilder;

    private boolean valuesOnly = false;

    private final Database db;

    TransactedSelectBuilder(SelectBuilder selectBuilder, Database db) {
        this.selectBuilder = selectBuilder;
        this.db = db;
    }

    public TransactedSelectBuilder parameters(Flowable<List<Object>> parameters) {
        selectBuilder.parameters(parameters);
        return this;
    }

    public TransactedSelectBuilder parameterList(List<Object> values) {
        selectBuilder.parameterList(values);
        return this;
    }

    public TransactedSelectBuilder parameterList(Object... values) {
        selectBuilder.parameterList(values);
        return this;
    }

    public TransactedSelectBuilder parameter(String name, Object value) {
        selectBuilder.parameter(name, value);
        return this;
    }

    public TransactedSelectBuilder parameters(Object... values) {
        selectBuilder.parameters(values);
        return this;
    }

    public TransactedSelectBuilder parameter(Object value) {
        selectBuilder.parameters(value);
        return this;
    }

    public TransactedSelectBuilder transactedValuesOnly() {
        this.valuesOnly = true;
        return this;
    }

    public TransactedSelectBuilderValuesOnly valuesOnly() {
        return new TransactedSelectBuilderValuesOnly(this, db);
    }

    public static final class TransactedSelectBuilderValuesOnly {
        private final TransactedSelectBuilder b;
        private final Database db;

        TransactedSelectBuilderValuesOnly(TransactedSelectBuilder b, Database db) {
            this.b = b;
            this.db = db;
        }

        public <T> Flowable<T> getAs(Class<T> cls) {
            return createFlowable(b.selectBuilder, cls, db) //
                    .flatMap(Tx.flattenToValuesOnly());
        }
    }

    public <T> Flowable<Tx<T>> getAs(Class<T> cls) {
        Flowable<Tx<T>> o = createFlowable(selectBuilder, cls, db);
        if (valuesOnly) {
            return o.filter(tx -> tx.isValue());
        } else {
            return o;
        }
    }

    private static <T> Flowable<Tx<T>> createFlowable(SelectBuilder sb, Class<T> cls, Database db) {
        return Flowable.defer(() -> {
            AtomicReference<Connection> connection = new AtomicReference<Connection>();
            return Select
                    .create(sb.connections.firstOrError() //
                            .map(c -> Util.toTransactedConnection(connection, c)), //
                            sb.parameterGroupsToFlowable(), //
                            sb.sql, //
                            sb.fetchSize, //
                            rs -> Util.mapObject(rs, cls, 1)) //
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
