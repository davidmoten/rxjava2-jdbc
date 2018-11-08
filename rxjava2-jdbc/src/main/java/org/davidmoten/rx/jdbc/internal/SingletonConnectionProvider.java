package org.davidmoten.rx.jdbc.internal;

import java.sql.Connection;

import org.davidmoten.rx.jdbc.ConnectionProvider;

public final class SingletonConnectionProvider implements ConnectionProvider {

    private final Connection connection;

    public SingletonConnectionProvider(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection get() {
        return connection;
    }

    @Override
    public void close() {
        // do nothing as con was not created by this provider
    }
}
