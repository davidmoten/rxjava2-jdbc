package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;

import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.MemberFactory;
import org.davidmoten.rx.pool.NonBlockingPool;

public class ConnectionNonBlockingMemberFactory
        implements MemberFactory<Connection, NonBlockingPool<Connection>> {

    @Override
    public Member<Connection> create(NonBlockingPool<Connection> pool) {
        return new ConnectionNonBlockingMember(pool);
    }

}
