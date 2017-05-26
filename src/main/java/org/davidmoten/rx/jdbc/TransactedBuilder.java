package org.davidmoten.rx.jdbc;

import java.sql.Connection;

import io.reactivex.Flowable;

public class TransactedBuilder {

    private final Flowable<Connection> connections;
    private final Database db;
    private SelectBuilder selectBuilder;

    public TransactedBuilder(TransactedConnection con, Database db) {
        this.db = db;
        this.connections = Flowable.just(con);
    }

    public TransactedSelectBuilder select(String sql) {
        this.selectBuilder = new SelectBuilder(sql, connections, db);
        return selectBuilder.transacted();
    }

}
