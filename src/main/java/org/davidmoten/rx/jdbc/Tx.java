package org.davidmoten.rx.jdbc;

import java.sql.Connection;

public interface Tx<T> {

    boolean isValue();

    boolean isComplete();

    boolean isError();

    T value();

    Throwable throwable();

    Connection connection();

    void commit();

    void rollback();
}