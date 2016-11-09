package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;

public class ConnectionNonBlockingMemberFactory
        implements MemberFactory<Connection, NonBlockingPool<Connection>> {

    @Override
    public Member<Connection> create(NonBlockingPool<Connection> pool) {
        return new ConnectionNonBlockingMember(pool);
    }

}
