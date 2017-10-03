package org.davidmoten.rx.pool;

public interface MemberFactory2<T, P extends Pool2<T>> {

    Member2<T> create(P pool);

}
