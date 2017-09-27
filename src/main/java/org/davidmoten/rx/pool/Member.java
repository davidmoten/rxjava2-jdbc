package org.davidmoten.rx.pool;

public interface Member<T> extends AutoCloseable {

    MemberWithValue<T> checkout();

    void checkin();

    void shutdown();

    boolean isShutdown();
}