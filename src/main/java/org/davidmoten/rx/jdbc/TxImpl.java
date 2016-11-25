package org.davidmoten.rx.jdbc;

import java.sql.Connection;

final class TxImpl<T> implements Tx<T> {

    private final TransactedConnection con;
    private final T value;
    private final Throwable e;
    private final boolean completed;

    TxImpl(Connection con, T value, Throwable e, boolean completed) {
        this.con = new TransactedConnection(con);
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
        return "TxImpl [con=" + con + ", value=" + value + ", e=" + e + ", completed=" + completed
                + "]";
    }
    
}
