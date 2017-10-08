package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import org.davidmoten.rx.pool.Member2;
import org.davidmoten.rx.pool.MemberWithValue2;
import org.davidmoten.rx.pool.Pool2;

import io.reactivex.Single;
import io.reactivex.plugins.RxJavaPlugins;

public final class ConnectionProviderBlockingPool2 implements Pool2<Connection> {

    private final ConnectionProvider connectionProvider;

    public ConnectionProviderBlockingPool2(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    @Override
    public Single<Member2<Connection>> member() {
        return Single.fromCallable(() -> new MemberWithValueConnection(connectionProvider));
    }

    @Override
    public void close() throws Exception {
        connectionProvider.close();
    }

    static final class MemberWithValueConnection
            implements MemberWithValue2<Connection>, DelegatedConnection {

        private final ConnectionProvider connectionProvider;

        public MemberWithValueConnection(ConnectionProvider cp) {
            this.connectionProvider = cp;
        }

        volatile Connection connection;
        final AtomicBoolean hasConnection = new AtomicBoolean();
        volatile boolean shutdown = false;

        @Override
        public Connection con() {
            if (hasConnection.compareAndSet(false, true)) {
                // blocking
                connection = connectionProvider.get();
            }
            return connection;
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
        public Connection value() {
            return connection;
        }

        @Override
        public void disposeValue() {
            try {
                connection.close();
            } catch (SQLException e) {
                RxJavaPlugins.onError(e);
            }
        }
    }
}
