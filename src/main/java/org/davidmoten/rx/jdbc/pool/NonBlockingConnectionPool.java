package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.Util;
import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.NonBlockingPool;
import org.davidmoten.rx.pool.Pool;

import io.reactivex.Flowable;

public class NonBlockingConnectionPool implements Pool<Connection> {

    private final NonBlockingPool<Connection> pool;

    public NonBlockingConnectionPool(ConnectionProvider cp, int maxSize, long retryDelayMs) {
        pool = NonBlockingPool.factory(() -> cp.get()) //
                .healthy(c -> true) //
                .disposer(Util::closeSilently) //
                .maxSize(maxSize) //
                .retryDelayMs(retryDelayMs) //
                .memberFactory(p -> new ConnectionNonBlockingMember(p)) //
                .build();
    }

    @Override
    public Flowable<Member<Connection>> members() {
        return pool.members();
    }

    @Override
    public void close() {
        pool.close();
    }

}
