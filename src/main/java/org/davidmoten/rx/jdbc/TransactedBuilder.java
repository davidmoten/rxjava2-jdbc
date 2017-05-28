package org.davidmoten.rx.jdbc;

import java.sql.Connection;

import io.reactivex.Flowable;

public final class TransactedBuilder {

    private final Flowable<Connection> connections;
    private final Database db;

    TransactedBuilder(TransactedConnection con, Database db) {
        this.db = db;
        this.connections = Flowable.just(con);
    }

    public TransactedSelectBuilder select(String sql) {
        return new SelectBuilder(sql, connections, db).transacted();
    }
    
    public TransactedUpdateBuilder update(String sql) {
        return new UpdateBuilder(sql, connections, db).transacted();
    }

}
