package org.davidmoten.rx.jdbc;

import java.sql.Connection;

import io.reactivex.Single;

public final class TransactedBuilder {

    private final Single<Connection> connections;
    private final Database db;

    TransactedBuilder(TransactedConnection con, Database db) {
        this.db = db;
        this.connections = Single.just(con);
    }

    public TransactedSelectBuilder select(String sql) {
        return new SelectBuilder(sql, connections, db).transacted();
    }
    
    public TransactedUpdateBuilder update(String sql) {
        return new UpdateBuilder(sql, connections, db).transacted();
    }

}
