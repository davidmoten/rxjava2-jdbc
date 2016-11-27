package org.davidmoten.rx.jdbc;

import java.sql.Connection;

import com.github.davidmoten.guavamini.Preconditions;

final class TxImpl<T> implements Tx<T> {

    private final TransactedConnection con;
    private final T value;
    private final Throwable e;
    private final boolean completed;

    TxImpl(Connection con, T value, Throwable e, boolean completed) {
        Preconditions.checkNotNull(con);
        if (con instanceof TransactedConnection) {
            this.con = (TransactedConnection) con;
        } else {
            this.con = new TransactedConnection(con);
        }
        this.value = value;
        this.e = e;
        this.completed = completed;
    }

    @Override
    public boolean isValue() {
        return !completed && e == null;
    }

    @Override
    public boolean isComplete() {
        return completed;
    }

    @Override
    public boolean isError() {
        return e != null;
    }

    @Override
    public T value() {
        return value;
    }

    @Override
    public Throwable throwable() {
        return e;
    }

    public TransactedConnection connection() {
        return con;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TxImpl [con=");
        builder.append(con);
        if (isValue()) {
            builder.append(", value=");
            builder.append(value);
        } else if (isError()) {
            builder.append(", e=");
            builder.append(e);
        } else if (isComplete()) {
            builder.append(", completed=");
            builder.append(completed);
        }
        builder.append("]");
        return builder.toString();
    }

}
