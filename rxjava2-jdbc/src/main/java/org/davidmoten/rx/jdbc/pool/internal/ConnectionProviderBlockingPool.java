package org.davidmoten.rx.jdbc.pool.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import org.davidmoten.rx.jdbc.internal.DelegatedConnection;
import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.Pool;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Single;
import io.reactivex.plugins.RxJavaPlugins;

public final class ConnectionProviderBlockingPool implements Pool<Connection> {

    private final ConnectionProvider connectionProvider;

    public ConnectionProviderBlockingPool(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    @Override
    public Single<Member<Connection>> member() {
        return Single.fromCallable(() -> new MemberWithValueConnection(connectionProvider));
    }

    @Override
    public void close() throws Exception {
        connectionProvider.close();
    }

    static final class MemberWithValueConnection
            implements Member<Connection>, DelegatedConnection {

        private final ConnectionProvider connectionProvider;

        public MemberWithValueConnection(ConnectionProvider cp) {
            this.connectionProvider = cp;
        }

        volatile Connection connection;
        final AtomicBoolean hasConnection = new AtomicBoolean();

        @Override
        public Connection con() {
            if (hasConnection.compareAndSet(false, true)) {
                // blocking
                connection = connectionProvider.get();
                // TODO remove precondition
                Preconditions.checkNotNull(connection, "connectionProvider should not return null");
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
            return con();
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
