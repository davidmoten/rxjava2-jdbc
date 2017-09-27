package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
import java.sql.SQLException;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.MemberWithValue;
import org.davidmoten.rx.pool.Pool;

import io.reactivex.Single;
import io.reactivex.plugins.RxJavaPlugins;

public final class ConnectionProviderBlockingPool implements Pool<Connection> {

    private final ConnectionProvider connectionProvider;

    public ConnectionProviderBlockingPool(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    @Override
    public Single<Member<Connection>> member() {
        return Single.fromCallable(() -> {
            return new MemberWithValue<Connection>() {

                Connection connection;
                volatile boolean shutdown = false;

                @Override
                public void close() throws Exception {
                    connectionProvider.close();
                }

                @Override
                public MemberWithValue<Connection> checkout() {
                    // blocking
                    connection = connectionProvider.get();
                    return this;
                }

                @Override
                public void checkin() {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        throw new SQLRuntimeException(e);
                    }
                }

                @Override
                public void shutdown() {
                    shutdown = true;
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        RxJavaPlugins.onError(e);
                    }
                }

                @Override
                public boolean isShutdown() {
                    return shutdown;
                }

                @Override
                public Connection value() {
                    return connection;
                }

            };
        });
    }

    @Override
    public void close() throws Exception {
        connectionProvider.close();
    }

}
