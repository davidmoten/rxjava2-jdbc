package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicReference;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.Util;
import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.NonBlockingPool;
import org.davidmoten.rx.pool.Pool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;

public class NonBlockingConnectionPool implements Pool<Connection> {

    private static final Logger log = LoggerFactory.getLogger(NonBlockingConnectionPool.class);

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
        return pool.get().members() //
                .doOnRequest(n -> log.debug("connections requested={}",n)) //
                .doOnNext(c -> log.debug("supplied {}", c));
    }

    @Override
    public void close() {
        pool.get().close();
    }

}
