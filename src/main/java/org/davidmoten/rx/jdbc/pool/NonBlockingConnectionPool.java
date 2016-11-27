package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicReference;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.Util;
import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.NonBlockingPool;
import org.davidmoten.rx.pool.Pool;

import io.reactivex.Flowable;

public class NonBlockingConnectionPool implements Pool<Connection> {

    private final AtomicReference<NonBlockingPool<Connection>> pool = new AtomicReference<NonBlockingPool<Connection>>();

    public NonBlockingConnectionPool(ConnectionProvider cp, int maxSize, long retryDelayMs) {
        pool.set(NonBlockingPool.factory(() -> cp.get()) //
                .healthy(c -> true) //
                .disposer(Util::closeSilently) //
                .maxSize(maxSize) //
                .retryDelayMs(retryDelayMs) //
                .memberFactory(p -> new ConnectionNonBlockingMember(pool.get())) //
                .build());
    }

    @Override
    public Flowable<Member<Connection>> members() {
        return pool.get().members();
    }

    @Override
    public void close() {
        pool.get().close();
    }

}
