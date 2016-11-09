package org.davidmoten.rx.jdbc.pool;

public interface MemberFactory<T, P extends Pool<T>> {

    Member<T> create(P pool);

}
