package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.Notification;

public class TransactedSelectBuilder {

    private static final Logger log = LoggerFactory.getLogger(TransactedSelectBuilder.class);

    private final SelectBuilder selectBuilder;

    private boolean valuesOnly = false;

    public TransactedSelectBuilder(SelectBuilder selectBuilder) {
        this.selectBuilder = selectBuilder;
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

    private static enum ValuesOnly {
        TRANSACTED_VALUES_ONLY, VALUES_ONLY, ALL;
    }

    public TransactedSelectBuilder valuesOnlyWithTx() {
        this.valuesOnly = true;
        return this;
    }

    public TransactedSelectBuilderValuesOnly valuesOnly() {
        return new TransactedSelectBuilderValuesOnly(this);
    }
    
    public static final class TransactedSelectBuilderValuesOnly {
        private final TransactedSelectBuilder b;

        TransactedSelectBuilderValuesOnly(TransactedSelectBuilder b) {
            this.b = b;
        }
        
        public <T> Flowable<T> getAs(Class<T> cls) {
            b.selectBuilder.resolveParameters();
            AtomicReference<Connection> connection = new AtomicReference<Connection>();
            Flowable<Tx<T>> o = Select.create(b.selectBuilder.connections.firstOrError() //
                    .map(c -> {
                        if (c instanceof TransactedConnection) {
                            connection.set(c);
                            return c;
                        } else {
                            c.setAutoCommit(false);
                            log.debug("creating new TransactedConnection");
                            TransactedConnection c2 = new TransactedConnection(c);
                            connection.set(c2);
                            return c2;
                        }
                    }), //
                    b.selectBuilder.parameters, //
                    b.selectBuilder.sql, //
                    b.selectBuilder.fetchSize, //
                    rs -> Util.mapObject(rs, cls, 1)) //
                    .materialize() //
                    .flatMap(n -> toTx(n, connection.get())).doOnNext(tx -> {
                        if (tx.isComplete()) {
                            ((TxImpl<T>) tx).connection().commit();
                        }
                    });
                return o.flatMap(Tx.flattenToValuesOnly());
        }

    }
    
    public <T> Flowable<Tx<T>> getAs(Class<T> cls) {
        selectBuilder.resolveParameters();
        AtomicReference<Connection> connection = new AtomicReference<Connection>();
        Flowable<Tx<T>> o = Select.create(selectBuilder.connections.firstOrError() //
                .map(c -> {
                    if (c instanceof TransactedConnection) {
                        connection.set(c);
                        return c;
                    } else {
                        c.setAutoCommit(false);
                        log.debug("creating new TransactedConnection");
                        TransactedConnection c2 = new TransactedConnection(c);
                        connection.set(c2);
                        return c2;
                    }
                }), //
                selectBuilder.parameters, //
                selectBuilder.sql, //
                selectBuilder.fetchSize, //
                rs -> Util.mapObject(rs, cls, 1)) //
                .materialize() //
                .flatMap(n -> toTx(n, connection.get())).doOnNext(tx -> {
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

    private static <T> Flowable<Tx<T>> toTx(Notification<T> n, Connection con) {
        if (n.isOnComplete())
            return Flowable.just(new TxImpl<T>(con, null, null, true));
        else if (n.isOnNext())
            return Flowable.just(new TxImpl<T>(con, n.getValue(), null, false));
        else
            return Flowable.error(n.getError());
    }

}
