package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.Notification;

public final class TransactedSelectBuilder {

    private static final Logger log = LoggerFactory.getLogger(TransactedSelectBuilder.class);

    private final SelectBuilder selectBuilder;

    private boolean valuesOnly = false;

    private final Database db;

    public TransactedSelectBuilder(SelectBuilder selectBuilder, Database db) {
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
            AtomicReference<Connection> connection = new AtomicReference<Connection>();
            Flowable<Tx<T>> o = Select
                    .create(b.selectBuilder.connections.firstOrError() //
                            .map(c -> Util.toTransactedConnection(connection, c)), //
                            b.selectBuilder.parameterGroupsToFlowable(), //
                            b.selectBuilder.sql, //
                            b.selectBuilder.fetchSize, //
                            rs -> Util.mapObject(rs, cls, 1)) //
                    .materialize() //
                    .flatMap(n -> toTx(n, connection.get(), db)).doOnNext(tx -> {
                        if (tx.isComplete()) {
                            ((TxImpl<T>) tx).connection().commit();
                        }
                    });
            return o.flatMap(Tx.flattenToValuesOnly());
        }
    }

    public <T> Flowable<Tx<T>> getAs(Class<T> cls) {
        AtomicReference<Connection> connection = new AtomicReference<Connection>();
        Flowable<Tx<T>> o = Select
                .create(selectBuilder.connections.firstOrError() //
                        .map(c -> Util.toTransactedConnection(connection, c)), //
                        selectBuilder.parameterGroupsToFlowable(), //
                        selectBuilder.sql, //
                        selectBuilder.fetchSize, //
                        rs -> Util.mapObject(rs, cls, 1)) //
                .materialize() //
                .flatMap(n -> toTx(n, connection.get(), db)).doOnNext(tx -> {
                    if (tx.isComplete()) {
                        ((TxImpl<T>) tx).connection().commit();
                    }
                });
        if (valuesOnly) {
            return o.filter(tx -> tx.isValue());
        } else {
            return o;
        }
    }

    private static <T> Flowable<Tx<T>> toTx(Notification<T> n, Connection con, Database db) {
        if (n.isOnComplete())
            return Flowable.just(new TxImpl<T>(con, null, null, true, db));
        else if (n.isOnNext())
            return Flowable.just(new TxImpl<T>(con, n.getValue(), null, false, db));
        else
            return Flowable.error(n.getError());
    }

}
