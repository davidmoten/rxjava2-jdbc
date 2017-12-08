package org.davidmoten.rx.jdbc;

import java.sql.Connection;

import javax.annotation.Nonnull;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Single;

public final class TransactedBuilder {

    private final Single<Connection> connections;
    private final Database db;

    TransactedBuilder(TransactedConnection con, Database db) {
        this.db = db;
        this.connections = Single.just(con);
    }

    public TransactedSelectBuilder select(@Nonnull String sql) {
        Preconditions.checkNotNull(sql, "sql cannot be null");
        return new SelectBuilder(sql, connections, db).transacted();
    }

    public TransactedUpdateBuilder update(@Nonnull String sql) {
        Preconditions.checkNotNull(sql, "sql cannot be null");
        return new UpdateBuilder(sql, connections, db).transacted();
    }

    public TransactedCallableBuilder call(@Nonnull String sql) {
        Preconditions.checkNotNull(sql, "sql cannot be null");
        return new CallableBuilder(sql, connections, db).transacted();
    }

}
