package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.Notification;

public class TransactedSelectBuilder {

    private final SelectBuilder selectBuilder;

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

    public <T> Flowable<Tx<T>> getAs(Class<T> cls) {
        selectBuilder.resolveParameters();
        AtomicReference<Connection> connection = new AtomicReference<Connection>();
        return Select
                .create(selectBuilder.connections.firstOrError() //
                        .doOnSuccess(c -> connection.set(c)), //
                        selectBuilder.parameters, //
                        selectBuilder.sql, //
                        rs -> Util.mapObject(rs, cls, 1)) //
                .materialize() //
                .map(n -> toTx(n, connection.get()));
    }

    private static <T> Tx<T> toTx(Notification<T> n, Connection con) {
        if (n.isOnComplete())
            return new TxImpl<T>(con, null, null, true);
        else if (n.isOnNext())
            return new TxImpl<T>(con, n.getValue(), null, false);
        else
            return new TxImpl<T>(con, null, n.getError(), false);
    }

}
